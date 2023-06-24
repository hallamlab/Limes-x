

import os
from pathlib import Path
import json
from typing import Any, Callable, Iterable, Literal

from .compute_module import ComputeModule
from .solver import DependencySolver, Plan
from .utils import KeyGenerator, StdTime

def _class_str(py_dtype: type):
    return str(py_dtype)[8:-2].split(".")[-1]

class Instance:
    WL = "py_dtype, dtype, val".split(", ")
    def __init__(self, py_dtype: type|str, dtype: str, val: Any) -> None:
        self.py_dtype = _class_str(py_dtype) if isinstance(py_dtype, type) else py_dtype
        self.dtype = dtype
        self.val = val

    def __str__(self) -> str:
        return f"I:{self.py_dtype}|{self.dtype}[{self.val}]"

    def __repr__(self) -> str:
        return f"{self}"

    def IsPyType(self, py_dtype: type|str):
        py_dtype = _class_str(py_dtype) if isinstance(py_dtype, type) else py_dtype
        return self.py_dtype == py_dtype

    @classmethod
    def Str(cls, dtype: str, val: str):
        return Instance(str, dtype, val)
    
    @classmethod
    def Path(cls, dtype: str, val: Path):
        return Instance(Path, dtype, val)
    
    @classmethod
    def ComputeModule(cls, val: ComputeModule):
        return Instance(ComputeModule, val.name, val)

    def ToDict(self):
        def serialize(v):
            if hasattr(v, "ToDict"):
                return v.ToDict()
            else:
                return str(v)
        return {k:serialize(v) for k, v in self.__dict__.items() if k in self.WL}
    
    @classmethod
    def FromDict(cls, data: dict):
        switch = {_class_str(k):v for k, v in [
            (str,            lambda d: str(d)),
            (Path,           lambda d: Path(d)),
            (ComputeModule,  lambda d: ComputeModule.FromDict(d)),
        ]}
        k_py_dtype = "py_dtype"
        loader = switch[data.get(k_py_dtype, _class_str(str))]
        return Instance(data[k_py_dtype], data["dtype"], loader(data["val"]))

LX_DIR = ".lx"
class ProjectState:
    FILE_NAME = "state.json"
    def __init__(self, workspace: Path|str, on_exist: Literal['error']|Literal['overwrite']|Literal['ignore']='error') -> None:
        workspace = Path(workspace)
        if workspace.exists():
            if on_exist == "error":
                assert False, f"workspace [{workspace}] exists"
            elif on_exist == "overwrite":
                os.system(f"rm -rf {workspace}")
        self._workspace:Path = workspace
        self._lx_dir = workspace.joinpath(LX_DIR)
        os.makedirs(self._lx_dir, exist_ok=True)

        self._key_gen = KeyGenerator()

        self._instances: dict[str, Instance] = {}
        self._plans: list[tuple[dict, Plan]] = []

    def Save(self):
        data = dict(
            instances = {k:v.ToDict() for k, v in self._instances.items()},
            plans = [(meta, p.ToDict()) for meta, p in self._plans]
        )
        with open(self._lx_dir.joinpath(self.FILE_NAME), "w") as f:
            json.dump(data, f, indent=4)

    @classmethod
    def Load(cls, workspace: Path|str):
        state = ProjectState(workspace, on_exist="ignore")
        with open(state._lx_dir.joinpath(state.FILE_NAME)) as f:
            data = json.load(f)
        state._instances = {k:Instance.FromDict(vd) for k, vd in data.get("instances", {}).items()}
        state._plans = [(meta, Plan.FromDict(d)) for meta, d in data.get("plans", [])]
        return state

    def RegisterInstance(self, i:Instance):
        self._instances[self._key_gen.GenerateUID(blacklist=self._instances)] = i

    def __getitem__(self, key: str):
        return self._instances[key]
    
    def GetInstance(self, key: str, default = None):
        return self._instances.get(key, default)

    def RegisterPlan(self, plan: Plan):
        meta = dict(time=StdTime.CurrentTimeMillis())
        self._plans.append((meta, plan))

    def ListData(self):
        return [(k, v) for k, v in self._instances.items() if not v.IsPyType(ComputeModule)]

    def ListDataTypes(self):
        return {d.dtype for k, d in self.ListData()}
    

# from __future__ import annotations
# from dataclasses import dataclass
# import os, sys
# from typing import Iterable, Any
# from pathlib import Path
# import pickle
# import json
# import gzip
# import hashlib

# class Instance:
#     def __init__(self, type: str, value: Any) -> None:
#         hash = hashlib.md5(pickle.dumps((type, value)))
#         self._id = int(hash.hexdigest(), 16)
#         self.type = type
#         self.value = value

#     def __eq__(self, __value: object) -> bool:
#         return isinstance(__value, Instance) and self._id == __value._id

#     def __hash__(self) -> int:
#         return self._id

# class ProjectState:
#     LX_DIR = ".lx"
#     COMPRESSION_LEVEL = 1
#     def __init__(self, workspace: Path|str) -> None:
#         self._lx_dir, self._save_file = self._get_paths(workspace)
#         self._instances: list[Instance] = []
#         self._lineage: dict[Instance, list[Instance]] = {}

#     @classmethod
#     def _get_paths(cls, workspace: Path|str):
#         workspace = Path(workspace)
#         lx_dir = workspace.joinpath(cls.LX_DIR)
#         save_file = lx_dir.joinpath("state.pkl.gz")
#         if not lx_dir.exists(): os.makedirs(lx_dir, exist_ok=True)
#         return lx_dir, save_file

#     def Save(self):
#         with gzip.open(self._save_file, "wb", compresslevel=self.COMPRESSION_LEVEL) as f:
#             pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

#     @classmethod
#     def Load(cls, workspace:Path|str) -> ProjectState:
#         lx_dir, save_file = cls._get_paths(workspace)
#         if save_file.exists():
#             with gzip.open(save_file, "rb") as f:
#                 return pickle.load(f)
#         else:
#             return ProjectState(workspace)

#     def GetDataTypes(self):
#         return 