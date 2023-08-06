from __future__ import annotations
from email.mime import application
import os
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any
from pathlib import Path
import yaml
import pickle, gzip, base64
from hypercorn.config import Config as HypercornConfig

from ..compute_module import ComputeModule
from ..models import Application, DataInstance, KeyGenerator, Transform


@dataclass
class Config:
    host: str = "localhost"
    port: int = 12100
    ver: str = "v1"
    home: Path = field(default_factory=lambda: Path("./"))
    compute_modules: list[Path] = field(default_factory=lambda: [Path("./compute_modules")])
    job_update_interval: int = 600 # 10 minutes

    def HypercornConfig(self):
        hc = HypercornConfig()
        hc.bind = [f"{self.host}:{self.port}"]
        return hc
    
    def Url(self):
        return f"http://{self.host}:{self.port}/{self.ver}"

    @classmethod
    def FromDict(cls, raw: dict, default: Config|None=None):
        if default is None: default = Config()
        c = Config() if default is None else default
        for k, v in c.__dataclass_fields__.items():
            if k not in raw: continue
            constr = {
                "port": int,
                "home": lambda p: Path(os.path.abspath(p)),
                "compute_modules": lambda ps: [Path(os.path.abspath(p)) for p in ps],
            }.get(v.name, str)
            setattr(c, k, constr(raw[k]))
        return c

    @classmethod
    def Load(cls, config_file: Path|str):
        config_file = Path(config_file)
        assert config_file.exists(), f"config [{config_file}] doesn't exist"
        assert config_file.is_file(),f"config [{config_file}] isn't a file"
        here = os.getcwd()
        os.chdir(config_file.parent)
        c = Config()
        try:
            with open(config_file.name) as y:
                d = yaml.safe_load(y)
                c = cls.FromDict(d, c)
        finally:
            os.chdir(here)
        return c

class Context(Enum):
    PING = auto()
    ERROR = auto()
    NOTICE = auto()
    RESPONSE = auto()
    SET_CONFIG = auto()
    RELOAD_MODULES = auto()
    LIST_TRANSFORMS = auto()
    REGISTER_PLAN = auto()

COMPRESSION_LEVEL = 3
_keygen = KeyGenerator(True)

@dataclass
class Message:
    context: Context
    payload: Any = None
    key: str = field(default_factory=lambda: _keygen.GenerateUID(12))

    def _pack(self, data: Any):
        return base64.urlsafe_b64encode(
            gzip.compress(
                pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL),
                compresslevel=COMPRESSION_LEVEL
            ),
        ).decode("ascii")
    
    def _unpack(self, raw: str):
        return pickle.loads(gzip.decompress(base64.urlsafe_b64decode(raw)))
    

    def Pack(self):
        raw = gzip.compress(
            pickle.dumps(self, protocol=pickle.HIGHEST_PROTOCOL),
            compresslevel=COMPRESSION_LEVEL
        )
        return base64.urlsafe_b64encode(raw).decode("ascii")
    
    @classmethod
    def Unpack(cls, raw: str) -> Message:
        raw_b = base64.urlsafe_b64decode(raw)
        return pickle.loads(gzip.decompress(raw_b))

@dataclass
class Plan:
    have: list[DataInstance]
    execution_order: list[Application]
    in_progress: list[Application] = field(default_factory=lambda: list())
    finished: list[Application] = field(default_factory=lambda: list())
