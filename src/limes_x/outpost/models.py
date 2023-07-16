from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

class Context(Enum):
    PING = auto()
    ERROR = auto()
    NOTICE = auto()
    RESPONSE = auto()
    SET_HOME = auto()
    RELOAD_MODULES = auto()
    LIST_TRANSFORMS = auto()
    REGISTER_JOB = auto()
    CANCEL_JOB  = auto()

@dataclass
class Message:
    key: str
    context: Context
    payload: Any

    @classmethod
    def FromDict(cls, raw: dict):
        kwargs = {}
        for k in cls.__dataclass_fields__:
            assert k in raw
            if k == "context":
                kwargs[k] = Context[raw[k]]
            else:
                kwargs[k] = raw[k]
        return Message(**kwargs)
    
    def ToDict(self):
        d = self.__dict__.copy()
        d["context"] = self.context.name
        return d