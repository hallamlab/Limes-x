from __future__ import annotations
from dataclasses import dataclass, field
import os, sys
from typing import Any, Iterable, Literal
import hashlib
import numpy as np
import json
from collections import deque

from limes_x.utils import KeyGenerator

class Namespace:
    def __init__(self) -> None:
        self.node_signatures: dict[int, str] = {}
        self._last_k: int = 0
        self._kg = KeyGenerator(True)
        self._KLEN = 4
        self._MAX_K = len(self._kg.vocab)**self._KLEN

    def NewKey(self):
        self._last_k += 1
        assert self._last_k < self._MAX_K
        return self._last_k, self._kg.FromInt(self._last_k, self._KLEN)

class Hashable:
    def __init__(self, ns: Namespace) -> None:
        self.namespace = ns
        self.hash, self.key = ns.NewKey()

    def __hash__(self) -> int:
        return self.hash
    
    def __eq__(self, __value: object) -> bool:
        K = "key"
        return hasattr(__value, K) and self.key == getattr(__value, K)

class Node(Hashable):
    def __init__(
        self,
        ns: Namespace,
        properties: set[str],
        parents: set[Node],
    ) -> None:
        super().__init__(ns)
        self.namespace = ns
        self.properties = properties
        self.parents = parents
        # self._diffs = set()
        # self._sames = set()

    def __str__(self) -> str:
        return f"<{self.key}:{','.join(self.properties)}>"

    def __repr__(self) -> str:
        return f"{self}"
    
    def IsA(self, other: Node, compare_lineage=False) -> bool:
        # if other.key in self._diffs: return False
        # if other.key in self._sames: return True
        if not other.properties.issubset(self.properties):
            # self._diffs.add(other.key)
            return False
        # self._sames.add(other.key)
        if compare_lineage: return not other.parents.issubset(self.parents)
        return True

    def Signature(self):
        cache = self.namespace.node_signatures
        if self.key not in cache:
            props = ",".join(sorted(self.properties))
            parents = ",".join(sorted([p.Signature() for p in self.parents]))
            sig = f"{props}-{parents}"
            cache[self.hash] = sig
        return cache[self.hash]

    def MatchesMemberOf(self, collection: Iterable[Node]):
        return any(self == m for m in collection)

class Dependency(Node):
    def __init__(self, namespace: Namespace, properties: set[str], parents: set[Node]) -> None:
        super().__init__(namespace, properties, parents)

class Endpoint(Node):
    def __init__(self, namespace: Namespace, properties: set[str], parents: set[Node]=set()) -> None:
        super().__init__(namespace, properties, parents)

class Transform(Hashable):
    def __init__(self, ns: Namespace) -> None:
        super().__init__(ns)
        self.requires: list[Dependency] = []
        self.produces: list[Dependency] = []
        self._ns = ns
        self._input_group_map: dict[int, list[Dependency]] = {}
        self._key = ns.NewKey()
        self._seen: set[str] = set()

    def __str__(self) -> str:
        def _props(d: Dependency):
            return "{"+"-".join(d.properties)+"}"
        return f"<{','.join(_props(r) for r in self.requires)}->{','.join(_props(p) for p in self.produces)}>"

    def __repr__(self): return f"{self}"

    def AddRequirement(self, properties: Iterable[str], parents: set[Dependency]=set()):
        return self._add_dependency(self.requires, properties, parents)

    def AddProduct(self, properties: Iterable[str], parents: set[Dependency]=set()):
        return self._add_dependency(self.produces, properties, parents)

    def _add_dependency(self, destination: list[Dependency], properties: Iterable[str], parents: set[Dependency]=set()):
        _parents: Any = parents
        _dep = Dependency(properties=set(properties), parents=_parents, namespace=self._ns)
        # assert not any(e.IsA(_dep) for e in destination), f"prev. dep ⊆ new dep"
        # assert not any(_dep.IsA(e) for e in destination), f"new dep ⊆ prev. dep "
        destination.append(_dep)
        if destination == self.requires:
            i = len(self.requires)-1
            for p in _parents:
                assert p in self.requires, f"{p} not added as a requirement"
            self._input_group_map[i] = self._input_group_map.get(i, [])+list(_parents)
        return _dep

    def _sig(self, endpoints: Iterable[Endpoint]):
        # return "".join(e.key for e in endpoints)
        return self.key+"-"+ "".join(e.key for e in endpoints)

    def Possibilities(self, have: Iterable[Endpoint]):
        matches: list[list[Endpoint]] = []
        for req in self.requires:
            _m = [m for m in have if m.IsA(req)]
            if len(_m) == 0: return []
            matches.append(_m)
        return matches

    def Apply(self, have: Iterable[Endpoint], use_signatures: set[str]) -> Iterable[Application]:
        matches = self.Possibilities(have)
        if len(matches) == 0: return []

        # can reduce exponential trial here by enforcning the input groups first
        def _possible_configs(i: int, choosen: list[Endpoint]) -> list[list[Endpoint]]:
            if i >= len(self.requires): return [choosen]
            candidates = matches[i]
            parents = self._input_group_map.get(i, [])
            # print(parents, candidates, choosen)
            if len(parents) > 0:
                for prototype in parents:
                    # parent must be in choosen, since it must have been added
                    # as a req. before being used as a parent
                    parent: None|Endpoint = None
                    for p in choosen:
                        if p.IsA(prototype): parent = p; break
                    if parent is None: return []
                    candidates = [c for c in candidates if parent in c.parents]
            configs = []
            for c in candidates:
                configs += _possible_configs(i+1, choosen+[c])
            return configs
        configs = _possible_configs(0, [])

        def _same(a: Endpoint, b: Endpoint):
            return a.properties.issubset(b.properties) and b.properties.issubset(a.properties) \
                and a.parents.issubset(b.parents) and b.parents.issubset(a.parents)

        for input_set in configs:
            sis = set(input_set)
            sig = self._sig(input_set)
            if sig in use_signatures: continue
            _parents = sis|{p for g in [e.parents for e in input_set] for p in g}
            produced = {
                Endpoint(
                    namespace=self._ns,
                    properties=out.properties,
                    parents=_parents
                )
            for out in self.produces}
            # if all(_same(e, p) for e in have for p in produced):
            #     continue
            #     print(have)
            #     print(produced)
            #     print()
            yield Application(self, sis, produced, sig)

@dataclass
class Application:
    transform: Transform
    used: set[Endpoint]
    produced: set[Endpoint]
    signature: str

@dataclass
class Result:
    solution: list[Application]
    message: str = ""
    info: Any = None
    steps: int = 0
    success: bool = False
    
def Solve(given: Iterable[Endpoint], target: Transform, transforms: Iterable[Transform]):
    @dataclass
    class State:
        have: set[Endpoint]
        plan: list[Application]
        usage_sigs: set[str]

    def _get_next_states(curr: State):
        for tr in transforms:
            for appl in tr.Apply(curr.have, curr.usage_sigs):
                yield State(
                    have = curr.have|appl.produced,
                    plan = curr.plan + [appl],
                    usage_sigs = curr.usage_sigs|{appl.signature},
                )
    
    def _check_done(curr: State):
        appls = target.Apply(curr.have, set())
        for a in appls:
            return a
    
    MAX_S = 100_000
    MAX_D = 32
    steps = 0
    def _solve(curr: State, depth: int, depth_lim: int) -> Result:
        nonlocal steps
        if depth>=MAX_D: return Result([], f"depth limit: {depth}")
        steps += 1
        if steps>MAX_S: return Result([], f"step limit: {steps}", curr, steps)

        final_appl = _check_done(curr)
        if final_appl is not None: return Result(curr.plan+[final_appl], steps=steps, success=True)

        for n in _get_next_states(curr):
            res = _solve(n, depth+1, depth_lim)
            if res.success: return res
        return Result([], "no sol", curr, steps)
    
    start = State(set(given), [], set())
    res = _solve(start, 0, MAX_D)
    # while res.success:
    #     _res = _solve(start, 0, len(res.solution))
    #     # _res = _solve(start, 0, 6)
    #     if not _res.success:
    #         print("no futher opt")
    #         break
    #     res = _res
    sol = res.solution
    last_l = 0
    while last_l != len(sol):
        last_l = len(sol)
        used = set()
        for a in sol:
            used |= a.used
        sol = [a for a in sol if a.transform==target or any(e in used for e in a.produced)]
    res.solution = sol
    return res
