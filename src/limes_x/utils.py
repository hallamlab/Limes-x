from typing import Iterable
import uuid
import time
from datetime import datetime as dt
import numpy as np

class PrivateInitException(Exception):
    def __init__(self) -> None:
        super().__init__(f'this class cant be initialized with a call, look for a classmethod')

class PrivateInit:
    _initializer_key: str = uuid.uuid4().hex

    def __init__(self, _key=None) -> None:
        if _key != self._initializer_key: raise PrivateInitException

class KeyGenerator:
    def __init__(self) -> None:
        ascii_vocab = [(48, 57), (65, 90), (97, 122)]
        vocab = [chr(i) for g in [range(a, b+1) for a, b in ascii_vocab] for i in g]
        # vocab += [c for c in "+="]
        self.vocab = vocab

    def GenerateUID(self, l:int=12, prefix: str="", blacklist: set[str]=set()) -> str:
        key: str|None = None
        while key is None or key in blacklist:
            digits = np.random.randint(0, len(self.vocab), l)
            key = prefix+"".join([self.vocab[i] for i in digits])
        blacklist.add(key)
        return key
    
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