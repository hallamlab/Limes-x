from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal
import hashlib
from collections import deque

class Namespace:
    def __init__(self) -> None:
        self.node_signatures: dict[int, str] = {}
        self._last_k: int = 0

    def NewKey(self):
        self._last_k += 1
        return self._last_k

class Hashable:
    def __init__(self, ns: Namespace) -> None:
        self.namespace = ns
        self.key = ns.NewKey()

    def __hash__(self) -> int:
        return self.key
    
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
        self._diffs = set()
        self._sames = set()

    def __str__(self) -> str:
        return f"<{self.key}:{','.join(self.properties)}>"

    def __repr__(self) -> str:
        return f"{self}"

    def IsA(self, other: Node) -> bool:
        if other.key in self._diffs: return False
        if other.key in self._sames: return True
        if not other.properties.issubset(self.properties):
            self._diffs.add(other.key)
            return False
        self._sames.add(other.key)
        # if not other.parents.issubset(self.parents): return False
        return True

    def Signature(self):
        cache = self.namespace.node_signatures
        if self.key not in cache:
            props = "".join(sorted(self.properties))
            parents = "".join(sorted([f">{p.Signature()}" for p in self.parents]))
            sig = props+parents
            cache[self.key] = sig
        return cache[self.key]

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
        return "".join(f'!{e.key}' for e in endpoints)

    def Apply(self, have: Iterable[Endpoint], blacklist: set[str]) -> list[Application]:
        matches: list[list[Endpoint]] = []

        for req in self.requires:
            _m = [m for m in have if m.IsA(req)]
            if len(_m) == 0: return []
            matches.append(_m)

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

        applications: list[Application] = []
        for input_set in configs:
            sis = set(input_set)
            sig = self._sig(input_set)
            if sig in blacklist: continue
            _parents = sis|{p for g in [e.parents for e in input_set] for p in g}
            produced = [
                Endpoint(
                    namespace=self._ns,
                    properties=out.properties,
                    parents=_parents
                )
            for out in self.produces]
            applications.append(Application(self, sis, produced, sig))
        return applications

@dataclass
class Application:
    transform: Transform
    used: set[Endpoint]
    produced: list[Endpoint]
    signature: str

@dataclass
class Result:
    solution: list[Application]
    message: str = ""
    evidence: Any = None
    steps: int = 0

def Solve(given: Iterable[Endpoint], target: Transform, transforms: Iterable[Transform]):
    @dataclass
    class State:
        have: list[Endpoint]
        usage_signatures: dict[int, set[str]]
        plan: list[Application]

    transforms = list(transforms)
    
    def _done(state: State):
        appl = target.Apply(state.have, set())
        return appl 

    def _solve() -> Result:
        MAXS = 2_000
        todo: deque[State] = deque([State(
            have = list(given),
            plan = [],
            usage_signatures={},
        )], maxlen=MAXS)

        def _deduplicate_states(current: State):
            def _get_sig(s: State):
                rsig = ''.join(f"{k}{''.join(v)}" for k, v in s.usage_signatures.items())
                sig = int(hashlib.md5(rsig.encode("latin1")).hexdigest(), 16)
                return sig
            seen = {_get_sig(current)}
            new_todo: deque[State] = deque([], MAXS)
            for s in todo:
                if _get_sig(s) in seen: continue
                new_todo.append(s)
            return new_todo
        
        _steps = 0
        _last_depth = 0
        _empty = set()
        while len(todo)>0:
            _steps += 1
            _s = todo.popleft()
            _depth = len(_s.plan)
            if _depth != _last_depth:
                todo = _deduplicate_states(_s)
                _last_depth = _depth
            if _steps > MAXS: return Result([], f"step limit exceeded", evidence=todo, steps=_steps)
            
            _target_applications = target.Apply(_s.have, _empty)
            if len(_target_applications)>0:
                return Result(solution=_s.plan+[_target_applications[0]], steps=_steps)

            if _done(_s): return Result(_s.plan, steps=_steps)
            for tr in transforms:
                possibilities = tr.Apply(_s.have, _s.usage_signatures.get(tr.key, set()))
                if len(possibilities) == 0: continue
                sigs = _s.usage_signatures.copy()
                new_have = _s.have.copy()
                for app in possibilities: # aggregate possiblities
                    sigs[tr.key] = sigs.get(tr.key, set())|{app.signature}
                    new_have += app.produced

                todo.append(State(
                    have = new_have,
                    plan = _s.plan+possibilities,
                    usage_signatures=sigs
                ))
        return Result([], f"ran out of things to try", steps = _steps)

    res = _solve()
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

#############################
## example

# NS = Namespace()
# def _set(s: str):
#     return set(s.split(", "))

# anner = Transform(NS)
# anner.AddRequirement(_set("annable"))
# anner.AddProduct(_set("ann"))

# taxer = Transform(NS)
# taxer.AddRequirement(_set("taxable"))
# taxer.AddProduct(_set("tax"))

# sumer = Transform(NS)
# d_parent = sumer.AddRequirement(_set("annable, taxable"))
# d_ann = sumer.AddRequirement(_set("ann"), {d_parent})
# d_tax = sumer.AddRequirement(_set("tax"), {d_parent})
# sumer.AddProduct(_set("sum"))

# M, N = 3, 1
# haves = [Endpoint(NS, _set(f"{i+1}, annable, taxable")) for i in range(M)]

# target = Transform(NS)
# for e in haves[:N]:
#     de = target.AddRequirement(e.properties)
#     target.AddRequirement(_set("sum"), {de})

# test_have = []
# for b in haves[:N]:
#     test_have.append(b)
#     test_have.append(Endpoint(NS, _set("ann"), {b}))
#     test_have.append(Endpoint(NS, _set("tax"), {b}))

# print("Start")
# r = Solve(haves, target, tr)
# f"input size [{N}], states checked [{r.steps}], {r.message}, {len(target.requires)}"