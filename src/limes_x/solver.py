from __future__ import annotations
from typing import Iterable, Literal, Any
from pathlib import Path

class Dependency:
    ATTRS_TO_PERSIST = set("requires, produces, ref_key".split(", "))
    def __init__(self, requires:Iterable[str], produces:Iterable[str], ref_key: str) -> None:
        self.requires = set(requires)
        self.produces = set(produces)
        self.ref_key = ref_key

    def __str__(self) -> str:
        return f"Dep:{self.ref_key}"
        # return f"{','.join(self.requires)}->{','.join(self.produces)}"

    def __repr__(self) -> str:
        return f"{self}"
    
    def ToDict(self):
        return {k:v for k, v in self.__dict__.items() if k in self.ATTRS_TO_PERSIST}

    @classmethod
    def FromDict(cls, data: dict):
        return Dependency(**{k:v for k, v in data.items() if k in cls.ATTRS_TO_PERSIST})

class _hashed_dependency:
    def __init__(self, key: int, ref: Dependency) -> None:
        self.key = key
        self.ref = ref

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, _hashed_dependency): return False
        return self.key == __o.key
    
    def __hash__(self) -> int:
        return self.key

class Plan:
    def __init__(self, execution_graph: Iterable[Dependency]) -> None:
        self._nodes = list(execution_graph)
        self._i = 0

    def __str__(self) -> str:
        return f"P:{self._nodes}"

    def __repr__(self) -> str:
        return f"{self}"
    
    def __getitem__(self, i: int):
        return self._nodes[i]
    
    def __iter__(self):
        self._i = -1
        return self
    
    def __next__(self):
        self._i += 1
        if self._i >= len(self._nodes):
            raise StopIteration
        else:
            return self._nodes[self._i]

    def ToDict(self):
        return dict(nodes=[n.ToDict() for n in self._nodes])

    @classmethod
    def FromDict(cls, data: dict):
        return Plan([Dependency.FromDict(n) for n in data.get("nodes", [])])

class DependencySolver:
    def __init__(self, dependencies: list[Dependency]) -> None:
        self.dependencies = dependencies
        self.production_map: dict[str, list[_hashed_dependency]] = {}
        i = 0
        for n in dependencies:
            hdep = _hashed_dependency(i, n)
            i += 1
            for prd in n.produces:
                self.production_map[prd] = self.production_map.get(prd, [])+[hdep]
        self._dependency_map: dict[int, list[_hashed_dependency]] = {}
        self._iteration_count = 0

    def Solve(self, given: Iterable[str], targets: Iterable[str]):
        def _solve(_given: set[str], _targets: set[str], _visited:list, _max_depth, _depth) -> list[_hashed_dependency]|Literal[False]:
            self._iteration_count += 1
            if _max_depth != 0 and _depth > _max_depth:
                return False
            if self._iteration_count >= 9999:
                return False
                
            res = []
            found = True
            for target in _targets-_given: # for each output
                if target not in self.production_map: # an output can't be made
                    found = False
                    return False
                paths = self.production_map[target]
                if len(paths) == 0: # an output does not need an input
                    continue

                best = []
                found_target = False # for this particular target
                for node in paths: # for each tool that can make this output
                    if node.key in _visited: continue

                    if node.key in self._dependency_map: # known: to use this tool, get known required sequence from dict
                        path = self._dependency_map[node.key]
                    elif node.ref.requires.issubset(_given): # tool input is given
                        path = [node]
                        self._dependency_map[node.key] = path
                    else:
                        path = _solve(_given, node.ref.requires, _visited+[node.key], len(best), _depth+1)
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
        
        self._dependency_map = {}
        self._iteration_count = 0
        result = _solve(set(given), set(targets), [], 0, 0)
        if not result: return False
        seen = set()
        unique_results: list[Dependency] = []
        for r in result:
            if r.key in seen: continue
            unique_results.append(r.ref)
            seen.add(r.key)

        # delete outputs that are
        # - produced multiple times
        # - not needed
        requires = set(targets)
        for step in unique_results:
            requires = requires.union(step.requires)
        created = set()
        for step in unique_results:
            step.produces = (step.produces - created).intersection(requires)
            created = created.union(step.produces)

        return Plan(unique_results)
