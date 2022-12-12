from __future__ import annotations
import os, sys
from pathlib import Path
from typing import Callable, Iterable
import json

from .solver import Transform, DependencySolver
from .common.utils import AutoPopulate, PrivateInit, LiveShell


# each item should be its own atomic thing... [thinking emoji]
class Item(PrivateInit):
    _instances: dict[str, Item] = {}
    
    @classmethod
    def Get(cls, key: str) -> Item:
        if key not in cls._instances:
            instance = Item(key, _ikey=cls._initializer_key)
            cls._instances[key] = instance
        return cls._instances[key]

    def __repr__(self) -> str:
        return f'<i:{self.key}>'

    def __init__(self, key: str, _ikey=None) -> None:
        super().__init__(_key=_ikey)
        self.key = key
        # self.persona_keys = {t.key for t in personas}

    # def CanBe(self, other: Item):
    #     return self.key in other.persona_keys

    # def GenerateFilePath(self, root: Path, file: Path):
    #     # return [root.joinpath(f) for f in files]
    #     return root.joinpath(file)

ManifestDict = dict[Item, Path]

class Params(AutoPopulate):
    # id: str
    threads: int
    mem_gb: int

    def Copy(self):
        cp = Params(**self.__dict__)
        return cp

class RunContext(AutoPopulate):
    params: Params
    shell: Callable[[str], int]
    output_folder: Path
    manifest: ManifestDict

class RunResult(AutoPopulate):
    exit_code: int
    manifest: ManifestDict|list[ManifestDict]

class ComputeModule:
    def __init__(self, procedure: Callable[[RunContext], RunResult], inputs: set[Item], outputs: set[Item]) -> None:
        self.name = procedure.__name__
        assert not Transform.Exists(self.name)
        assert len(inputs.intersection(outputs)) == 0
        self.inputs = inputs
        self.outputs = outputs
        self._procedure = procedure

    def __repr__(self) -> str:
        return f'<m:{self.name}>'

    def GetTransform(self):
        if Transform.Exists(self.name):
            return Transform.Get(self.name)
        else:
            return Transform.Create(
                {x.key for x in self.inputs},
                {x.key for x in self.outputs},
                unique_name=self.name,
                reference=self,
            )

    def Run(self, context: RunContext):
        # p = Params()
        # p.shell = lambda cmd: CondaShell(f'{self.name}', cmd)
        return self._procedure(context)

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

    def Run(self, params: Params, workspace: str|Path, targets: Iterable[Item]):
        if isinstance(workspace, str): workspace = Path(workspace)
        original_dir = os.getcwd()
        os.chdir(workspace)

        # todo: extract responsibility to dedicated class
        def _conda_shell(cmd):
            return LiveShell(f'conda run --no-capture-output -n flux_runtime {cmd}')
            # print(cmd)
            # return 0

        state = WorkflowState(workspace)
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
                shell=_conda_shell,
                output_folder = Path(this_step.name),
                params=params.Copy(),
                manifest=state.GetNamespace(namespace),
            )

            print(f"namespace: {'.'.join(_namespace) if len(_namespace)>0 else 'root'}, running step: {this_step.name}")
            result = this_step.Run(context)
            # print()

            if result.exit_code != 0:
                raise RunError(f"{this_step.name} failed with code {result.exit_code}")

            if isinstance(result.manifest, dict):
                state.Update(namespace, result.manifest)
                if this_step in nexts: todo.append((_namespace, nexts[this_step]))
            else: # split
                for j, m in enumerate(result.manifest):
                    new_ns = _namespace+[f'{this_step.name}_{j+1}']
                    state.Update(tuple(new_ns), m)
                    if this_step in nexts: todo.append((new_ns, nexts[this_step]))

            state.Save()

        os.chdir(original_dir)
