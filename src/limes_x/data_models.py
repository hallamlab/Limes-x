from __future__ import annotations
from typing import Iterable

class Instance:
    def __init__(self, key: str, parent: Instance|None=None) -> None:
        self.key = key
        self.parent = parent

class RunInstance(Instance):
    pass

class DataInstance(Instance):
    def __init__(self, key: str, dtype: str, value: str|Iterable[str], parent: Instance|None=None) -> None:
        super().__init__(key, parent)
        self.dtype = dtype
        self.value = value
        self.parent = parent
