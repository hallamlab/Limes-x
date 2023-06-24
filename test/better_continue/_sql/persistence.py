from __future__ import annotations
import os, sys
from typing import Iterable, Any
from pathlib import Path
import pickle
import sqlite3

from .common.utils import KeyGenerator, StdTime
from .data_models import Instance, DataInstance, RunInstance

class Table:
    def __init__(self, state: ProjectState, name: str, fields: list[tuple[str, str]], foreign_keys: list[tuple[str, str, str]]=list()) -> None:
        self._state = state
        self._name = name
        self._pk, self._pk_type = fields[0]
        self._fields = fields
        self._keygen = KeyGenerator()

        params = [f"{k} {t}" for k, t in fields]
        params += [f"PRIMARY KEY ({self._pk})"]
        params += [f"FOREIGN KEY ({k}) REFERENCES {table} ({other})" for k, table, other in foreign_keys]
        state._execute_sql(f"""\
            CREATE TABLE IF NOT EXISTS {self._name} ({','.join(params)})
        """)

    def KeyExists(self, key:str):
        result = self._state._execute_sql(f"""\
            SELECT EXISTS(SELECT 1 FROM {self._name} WHERE {self._pk}=?);
        """, (key,))
        result = next(iter(result))
        return bool(result[0])

    def _new_key(self):
        while True:
            k = self._keygen.GenerateUID()
            if not self.KeyExists(k): break
        return k
    
    def _insert(self, entry: dict):
        self._state._execute_sql(f"""\
            INSERT INTO {self._name} ({','.join(entry)}) VALUES ({','.join(['?' for _ in entry.values()])})
        """, tuple(entry.values()))

    def _get(self, key: str):
        candidates = self._state._execute_sql(f"""\
            SELECT * FROM {self._name} WHERE {self._pk}=?
        """, (key,))
        return next(candidates)

class ObjectTable(Table):
    def __init__(self, name: str, state: ProjectState) -> None:
        super().__init__(
            state,
            name,
            [
                ("key",             "TEXT"),
                ("date",            "INTEGER"),
                ("pickle",          "BLOB"),
            ],
        )

    def Add(self, val: Any):
        key = self._new_key()
        sval = pickle.dumps(val, pickle.HIGHEST_PROTOCOL)
        entry = {
            "key":key,
            "date":StdTime.CurrentTimeMillis(),
            "pickle":sval,
        }
        self._insert(entry)
        return key

    # returns meta, value
    def Get(self, key: str):
        entry = self._get(key)
        if entry is None: return None
        key, sdate, sval = entry
        return dict(
            key = key,
            date = StdTime.Parse(sdate),
        ), pickle.loads(sval)

class IndexTable(Table):
    def __init__(self, name: str, state: ProjectState) -> None:
        super().__init__(
            state,
            name,
            [
                ("entry",       "TEXT"),
                ("a",           "TEXT"),
                ("b",           "TEXT"),
            ],
        )
        self._cache: dict[str, str] = {}

    def __getitem__(self, k: str):
        pass

    def __setitem__(self, source: str, target: str):
        self._cache[source] = target
        _e = self._new_key()
        entry = {
            "entry":_e,
            "a":source,
            "b":target,
        }
        self._insert(entry)

    def Add(self, source: str, target: str):

    def Get(self, key: str):
        entry = self._get(key)
        if entry is None: return None
        key, sval = entry
        return dict(
            key = key,
        ), pickle.loads(sval)

LX_DIR = ".lx"
class ProjectState:
    def __init__(self, workspace: Path|str) -> None:
        workspace = Path(workspace)
        self._lx_dir = workspace.joinpath(LX_DIR)
        if not self._lx_dir.exists(): os.makedirs(self._lx_dir, exist_ok=True)
        self._con: sqlite3.Connection|None = None
        self._cur: sqlite3.Cursor|None = None
        self._connected = False

    def __enter__(self):
        self._con = sqlite3.connect(self._lx_dir.joinpath("state.db"))
        self._cur = self._con.cursor()
        self._connected = True
        self._t_data = ObjectTable("data", self)
        self._t_dtypes = IndexTable("data_types", self)
        self._t_parents = IndexTable("parents", self)
        return self

    def __del__(self):
        if self._cur is not None: self._cur.close()
        if self._con is not None:
            self._con.commit()
            self._con.close()
        self._connected = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

    def _execute_sql(self, sql: str, values: tuple|None=None):
        assert self._cur is not None
        return self._cur.execute(sql) if values is None else self._cur.execute(sql, values)
    
    def _assert_connected(self):
        assert self._connected, f'usage: "with ProjectState() as state"'

    def _register_data(self, dtype: str, value: str|Iterable[str], parent: Instance|None=None):
        k = self._t_data.Add(value)
        if parent is not None:
            self._t_parents.Add(k)