import os, sys
from pathlib import Path
from typing import Callable, Iterable, Any, Literal
import json

from .solver import DependencySolver
from .common.utils import AutoPopulate, PrivateInit, LiveShell
from .compute_module import Item, ComputeModule, Params, RunContext
from .executors import Executor

class RunError(Exception):
     def __init__(self, message=""):
        self.message = message
        super().__init__(self.message)

class WorkflowState:
    _FILE_NAME = 'manifest.json'
    ROOT: str = "root"

    def __init__(self, workspace: str|Path) -> None:
        if isinstance(workspace, str):
            workspace = Path(workspace)

        manifest = {}
        namespaces: dict[str, dict] = {} # points to levels in manifest
        ns_parents: dict[str, str] = {}
        manifest_path = workspace.joinpath(self._FILE_NAME)
        seen_ns_keys = set()
        if os.path.exists(manifest_path):
            with open(manifest_path) as j:
                _m = json.load(j)

                def _check(parent_k: str, raw_loaded: dict):
                    if parent_k in seen_ns_keys:
                        raise ValueError(f"namespace [{parent_k}] has duplicates")
                    seen_ns_keys.add(parent_k)

                    local_ns = {}
                    for k, entry in raw_loaded.items():
                        if isinstance(entry, str):
                            check_path = workspace.joinpath(entry)
                            if not check_path.exists():
                                print(f"warning: file at path doesn't exist {check_path}")
                                continue
                            ik, p = Item.Get(k), Path(entry)
                            local_ns[ik] = p
                        else: # type is dict, found chile namespace
                            sub_ns = _check(k, entry)
                            local_ns[k] = sub_ns
                            ns_parents[k] = parent_k
                    namespaces[parent_k] = local_ns
                    return local_ns
                manifest = _check(self.ROOT, _m)
        else:
            namespaces[self.ROOT] = manifest

        self._workspace = workspace
        self._manifest = manifest
        self._namespaces = namespaces
        self._ns_parents = ns_parents

    def NamespaceExists(self, key: str):
        return key in self._namespaces

    def NewNamespace(self, key: str, parent_key: str|None = None):
        if parent_key is None: parent_key = self.ROOT
        assert self.NamespaceExists(parent_key)
        assert not self.NamespaceExists(key)

        new = {}
        self._namespaces[parent_key][key] = new
        self._namespaces[key] = new
        self._ns_parents[key] = parent_key

    def GetNamespace(self, key: str) -> dict[Item, Path]:
        if not self.NamespaceExists(key): return {}
        
        visible = self._namespaces[key].copy()
        namespaces = []
        while True:
            if key not in self._ns_parents: break
            pkey = self._ns_parents[key]
            namespaces.append(self._namespaces[pkey])
            key = pkey

        for ns in namespaces:
            for k, v in ns.items():
                if not isinstance(k, Item): continue
                visible[k] = v
        return visible

    def GetRoot(self):
        return self.GetNamespace(self.ROOT)

    def Update(self, namespace: str, content: dict[Item, Path]):
        assert namespace in self._namespaces
        self._namespaces[namespace].update(content)

    def Set(self, namespace: str, key: Item, value: Path):
        self.Update(namespace, {key: value})

    def Save(self):
        def _stringify(man: dict):
            str_man = {}
            for k, v in man.items():
                if isinstance(k, Item):
                    str_man[k.key] = str(v)
                else:
                    str_man[k] = _stringify(v)
            return str_man
        stringyfied = _stringify(self._manifest) 
        with open(self._workspace.joinpath(self._FILE_NAME), 'w') as j:
            json.dump(stringyfied, j, indent=4)

class Workflow:
    def __init__(self, compute_modules: list[ComputeModule]) -> None:
        self._compute_modules = compute_modules
        # self._solver = DependencySolver([c.GetTransform() for c in compute_modules])
        self._solver = DependencySolver([c.GetTransform() for c in compute_modules])

    def _calculate(self, given: Iterable[Item], targets: Iterable[Item]):
        given_k = {x.key for x in given}
        targets_k = {x.key for x in targets} 
        steps = self._solver.Solve(given_k, targets_k)
        return steps

    def _run(self, steps: list[ComputeModule], state: WorkflowState):
        pass

    # this just does setup, _run actually executes the compute modules
    def Run(self, params: Params, workspace: str|Path, targets: Iterable[Item], given: dict[Item, str|Path], executor: Executor):
        if isinstance(workspace, str): workspace = Path(workspace)
        if not workspace.exists():
            os.makedirs(workspace)

        # abs. path before change to working dir
        sys.path = [os.path.abspath(p) for p in sys.path]
        abs_given = dict((k, os.path.abspath(p)) for k, p in given.items())

        original_dir = os.getcwd()
        try:
            os.chdir(workspace)

            # initialize state (with inputs and existing files)
            state = WorkflowState('./')
            inputs = state.GetRoot()
            if len(abs_given) > 0:
                input_dir = Path("./inputs")
                os.makedirs(input_dir, exist_ok=True)
                for k, p in abs_given.items():
                    p = Path(p)
                    linked = input_dir.joinpath(p.name)
                    if linked.exists(): os.remove(linked)
                    os.symlink(p, linked)
                    inputs[k] = linked
                state.Update(state.ROOT, inputs)
                state.Save()

            calculated_order = self._calculate(inputs.keys(), targets)
            if calculated_order is False:
                os.chdir(original_dir)
                print('no solution')
                return
            if len(calculated_order) == 0:
                os.chdir(original_dir)
                print('nothing to do')
                return

            steps: list[ComputeModule] = []
            for s in calculated_order:
                _cm = s.reference
                assert isinstance(_cm, ComputeModule)
                steps.append(_cm)

            self._run(steps, state)

        finally:
            os.chdir(original_dir)



            # nexts = dict((steps[i], steps[i+1]) for i, _ in enumerate(steps) if i<len(steps)-1)
            # # print(nexts, steps)

            # todo: list[tuple[str, ComputeModule]] = [
            #     (state.ROOT, steps[0])
            # ]

            # while len(todo) > 0:
            #     namespace, this_step = todo.pop(0)

            #     context = RunContext(
            #         output_folder = Path(this_step.name),
            #         params=params.Copy(),
            #         manifest=state.GetNamespace(namespace),
            #     )

            #     print(f"namespace: {namespace}, running step: {this_step.name}")
            #     result = executor.Run(this_step, context)

            #     if result.exit_code != 0:
            #         raise RunError(f"{this_step.name} failed with code {result.exit_code}")
            #     def check_outputs(man: dict):
            #         for path in man.values():
            #             assert os.path.exists(path), f"promised output [{path}] doesn't exist"

            #     if isinstance(result.manifest, dict):
            #         check_outputs(result.manifest)
            #         state.Update(namespace, result.manifest)
            #         if this_step in nexts: todo.append((_namespace, nexts[this_step]))
            #     else: # multiple outputs, so split
            #         for j, m in enumerate(result.manifest):
            #             check_outputs(m)
            #             new_ns = _namespace+[f'{this_step.name}_{j+1}']
            #             state.Update(tuple(new_ns), m)
            #             if this_step in nexts: todo.append((new_ns, nexts[this_step]))

            #     state.Save()