import os, sys
from pathlib import Path
from typing import Callable, Iterable, Any, Literal
import json

from .solver import DependencySolver
from .common.utils import AutoPopulate, PrivateInit, LiveShell
from .compute_module import Item, ComputeModule, Params, RunContext
from .executors import Executor

class RunError(Exception):
    pass

class WorkflowState:
    FILE_NAME = 'manifest.json'
    SEP = '.'

    def __init__(self, workspace: str|Path) -> None:
        if isinstance(workspace, str):
            workspace = Path(workspace)

        def _parse_ns(ns: str):
            if ns=='':
                return ()
            else:
                return tuple(ns.split(self.SEP))

        manifest: dict[tuple, dict[Item, Path]] = {}
        with open(workspace.joinpath(self.FILE_NAME)) as j:
            _m: dict = json.load(j)
            for ns, files in _m.items():
                sub_man = {}
                for k, v in files.items():
                    item = Item.Get(k)
                    if item is None:
                        print(f"warning: unrecognized manifest entry: {k}")
                        continue
                    path = Path(v)
                    check_path = workspace.joinpath(path)
                    if not check_path.exists():
                        print(f"warning: file at path doesn't exist {check_path}")
                        continue

                    sub_man[item] = path
                manifest[_parse_ns(ns)] = sub_man

        self._workspace = workspace
        self._manifest = manifest

    def GetNamespace(self, namespace: tuple) -> dict[Item, Path]:
        visible = {}
        while True:
            visible.update(self._manifest[namespace])
            _namesp_to_add = namespace[:-1]
            if len(_namesp_to_add) == 0: break
        return visible

    def Update(self, namespace: tuple, content: dict[Item, Path]):
        d = self._manifest.get(namespace, {})
        d.update(content)
        self._manifest[namespace] = d

    def Set(self, namespace: tuple, key: Item, value: Path):
        d = self._manifest.get(namespace, {})
        d[key] = value
        self._manifest[namespace] = d

    def Save(self):
        str_man = {}
        for ns, d in self._manifest.items():
            sub_man = {}
            for item, path in d.items():
                sub_man[item.key] = str(path)
            str_man[self.SEP.join(ns)] = sub_man

        with open(self.FILE_NAME, 'w') as j:
            json.dump(str_man, j, indent=4)

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

    # todo: extract responsibility to dedicated class
    def _conda_shell(self, cmd):
            return LiveShell(f'conda run --no-capture-output -n flux_runtime {cmd}')

    def Run(self, params: Params, workspace: str|Path, targets: Iterable[Item], executor: Executor):
        if isinstance(workspace, str): workspace = Path(workspace)
        original_dir = os.getcwd()
        try:
            os.chdir(workspace)

            state = WorkflowState(Path("./"))
            inputs = state.GetNamespace(())
            calculated_order = self._calculate(inputs.keys(), targets)

            if calculated_order is False:
                print('no solution')
                return
            if len(calculated_order) == 0:
                print('nothing to do')
                return

            steps: list[ComputeModule] = []
            for s in calculated_order:
                _cm = s.reference
                assert isinstance(_cm, ComputeModule)
                steps.append(_cm)
            nexts = dict((steps[i], steps[i+1]) for i, _ in enumerate(steps) if i<len(steps)-1)
            # print(nexts, steps)

            todo: list[tuple[list, ComputeModule]] = [
                ([], steps[0])
            ]
            while len(todo) > 0:
                _namespace, this_step = todo.pop(0)
                namespace = tuple(_namespace)

                context = RunContext(
                    shell=self._conda_shell,
                    output_folder = Path(this_step.name),
                    params=params.Copy(),
                    manifest=state.GetNamespace(namespace),
                )

                print(f"namespace: {'.'.join(_namespace) if len(_namespace)>0 else 'root'}, running step: {this_step.name}")
                result = executor.Run(this_step, context)
                # print()

                if result.exit_code != 0:
                    raise RunError(f"{this_step.name} failed with code {result.exit_code}")
                def check_outputs(man: dict):
                    for path in man.values():
                        assert os.path.exists(path), f"promised output [{path}] doesn't exist"

                if isinstance(result.manifest, dict):
                    check_outputs(result.manifest)
                    state.Update(namespace, result.manifest)
                    if this_step in nexts: todo.append((_namespace, nexts[this_step]))
                else: # split
                    for j, m in enumerate(result.manifest):
                        check_outputs(m)
                        new_ns = _namespace+[f'{this_step.name}_{j+1}']
                        state.Update(tuple(new_ns), m)
                        if this_step in nexts: todo.append((new_ns, nexts[this_step]))

                state.Save()
        finally:
            os.chdir(original_dir)
