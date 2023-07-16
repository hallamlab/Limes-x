from typing import Iterable
import uuid
import time
from datetime import datetime as dt
import numpy as np
import os
from pathlib import Path

def GetModuleRoot():
    return Path("/".join(os.path.realpath(__file__).split('/')[:-1]))

def Version():
    with open(GetModuleRoot().joinpath("version.txt")) as v:
        return v.readline()

class PrivateInitException(Exception):
    def __init__(self) -> None:
        super().__init__(f'this class cant be initialized with a call, look for a classmethod')

class PrivateInit:
    _initializer_key: str = uuid.uuid4().hex

    def __init__(self, _key=None) -> None:
        if _key != self._initializer_key: raise PrivateInitException
    
class StdTime:
    FORMAT = '%Y-%m-%d_%H-%M-%S'

    @classmethod
    def Timestamp(cls, timestamp: dt|None = None):
        ts = dt.now() if timestamp is None else timestamp
        return f"{ts.strftime(StdTime.FORMAT)}"
    
    @classmethod
    def Parse(cls, timestamp: str|int):
        if isinstance(timestamp, str):
            return dt.strptime(timestamp, StdTime.FORMAT)
        else:
            return dt.fromtimestamp(timestamp/1000)
    
    @classmethod
    def CurrentTimeMillis(cls):
        return round(time.time() * 1000)