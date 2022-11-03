from __future__ import annotations
import os
import json

from .compute_modules import ComputeModule
from .common.utils import _remove_trailing_slash, Abstract
from .constants import DP_FOLDER
from data_pointer import DataPointer

class Workflow:
    def __init__(self, compute_modules_path: str, engine: AbstractEngine | None=None) -> None:

        compute_modules_path = _remove_trailing_slash(compute_modules_path)
        self.compute_modules: list[ComputeModule] = []
        for folder in os.listdir(compute_modules_path):
            module_path = f'{compute_modules_path}/{folder}'
            module = ComputeModule.LoadFromDisk(module_path)
            if module is not None:
                self.compute_modules.append(module)

        if engine is None: engine = DefaultEngine(self)
        self.engine = engine

    def Run(self, workspace: str, targets: list[str]):
        workspace = os.path.abspath(workspace)
        if not os.path.isdir(workspace):
            os.mkdir(workspace)
        self.engine.Run(workspace, targets)

class AbstractEngine(Abstract):
    def __init__(self, workflow: Workflow,_key=None) -> None:
        super().__init__(_key)
        self.workflow = workflow

    def Run(self, workspace: str, targets: list[str]):
        pass

class SnakemakeEngine(AbstractEngine):
    CONTAINER = '/home/tony/workspace/singularity/WF/snakemake/'
    def __init__(self, workflow: Workflow) -> None:
        super().__init__(workflow, self._abstract_initializer_key)

    def Run(self, workspace: str, targets: list[str]):
        workspace = _remove_trailing_slash(workspace)
        module_names = set()
        T = '\t'
        ext = DataPointer.EXT
        def _make_rule(m: ComputeModule):
            name = m.name
            i = 2
            while name in module_names:
                name = f'{m.name}_{i}'
                i+=1

            return "\n".join([
                f'rule {m.name}:',
                f'{T}input:',
                ",\n".join([f'{T}{T}"{DP_FOLDER}/{dtype}.{ext}"' for dtype in m._inputs]),
                f'{T}output:',
                ",\n".join([f'{T}{T}"{DP_FOLDER}/{dtype}.{ext}"' for dtype in m._outputs]),
                f'{T}shell:',
                f'{T}{T}"{m.GetEntryPoint()} {workspace}"'
            ])

        def _make_targets():
            assert 'all' not in module_names, "[all] is not a valid module name, rename that folder"
            return "\n".join([
                f'rule all:',
                f'{T}input:',
                ",\n".join([f'{T}{T}"{DP_FOLDER}/{dtype}.{ext}"' for dtype in targets]),
            ])

        with open(f'{workspace}/snakefile', 'w') as sf:
            sm_config = f'{_make_targets()}\n\n'
            for m in self.workflow.compute_modules:
                sm_config += f'{_make_rule(m)}\n\n'
            sf.write(sm_config)
        os.system(f'singularity run -B {workspace}:/data {self.CONTAINER}')

DefaultEngine = SnakemakeEngine
