from __future__ import annotations
# from cobra import Model, Reaction, Metabolite
# import networkx as nx
from typing import Literal, Any
import uuid

from ..common.utils import PrivateInit

class Transform(PrivateInit):
    _last_i = 0
    _keys: dict[str, int] = {}
    _instances: dict[str, Transform] = {}

    @classmethod
    def _new_id(cls, k: str):
        id = cls._last_i
        cls._last_i += 1
        cls._keys[k] = id
        return id

    @classmethod
    def Create(cls, ins:set[str], outs:set[str], unique_name: str|None=None, reference=None):
        if unique_name is None:
            while True:
                unique_name = uuid.uuid4().hex
                if unique_name not in cls._instances: break

        assert unique_name not in cls._instances
        t = Transform(ins, outs, unique_name=unique_name, _k = cls._initializer_key)
        t.reference = reference
        return t

    @classmethod
    def Get(cls, unique_name: str):
        return cls._instances[unique_name]

    @classmethod
    def Exists(cls, name: str):
        return name in cls._instances

    def Unregister(self):
        del self._instances[self.key]
        del self._keys[self.key]

    def __init__(self, ins:set[str], outs:set[str], unique_name: str, _k=None) -> None:
        super().__init__(_k)
        self._id = Transform._new_id(unique_name)
        self.key = unique_name

        self.ins = ins
        self.outs = outs
        self.reference: Any = None

    def __repr__(self) -> str:
        return f'tr:{self.key}'

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Transform): return False
        return self.key == __o.key

    def __hash__(self) -> int:
        return self._id

class DependencySolver:
    def __init__(self, nodes: list[Transform]) -> None:
        self.nodes = nodes
        self.production_map: dict[str, list[Transform]] = {}
        for n in nodes:
            for prd in n.outs:
                self.production_map[prd] = self.production_map.get(prd, [])+[n]
        self._dependency_map: dict[str, list[Transform]] = {}
        self._c = 0

    def Solve(self, given: set[str], targets: set[str]):
        self._dependency_map = {}
        self._c = 0
        result = self._solve(given, targets, [], 0, 0)
        dep_map = {}
        for k, v in self._dependency_map.items():
            dep_map[k] = [t.reference if t.reference is not None  else t.key for t in v]
        return result, dep_map
        
    def _solve(self, given: set[str], targets: set[str], visited:list, max_depth, depth) -> list[Transform]|Literal[False]:
        self._c += 1
        if max_depth != 0 and depth > max_depth:
            return False
        if self._c >= 9999:
            return False
            
        res = []

        found = True
        for target in targets-given: # for each output
            if target not in self.production_map: # an output can't be made
                found = False
                return False
            paths = self.production_map[target]
            if len(paths) == 0: # an output does not need an input
                continue

            best = []
            found_target = False # for this particular target
            for node in paths: # for each tool that can make this output
                if node.key in visited: continue

                if node.key in self._dependency_map: # known: to use this tool, get known required sequence from dict
                    path = self._dependency_map[node.key]
                elif node.ins.issubset(given): # tool input is given
                    path = [node]
                    self._dependency_map[node.key] = path
                else:
                    path = self._solve(given, node.ins, visited+[node.key], len(best), depth+1)
                    if path == False: continue

                    if node not in path: path.append(node)
                    self._dependency_map[node.key] = path
                if len(best)==0 or len(path)<len(best):
                    found_target = True
                    best = path

            found = found and found_target
            res = res + best

        if found:
            _found = set()
            result = []
            for r in res:
                if r in _found: continue
                result.append(r)
                _found.add(r)
            return result
        return False   
