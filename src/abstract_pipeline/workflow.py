from __future__ import annotations
import sys, os
import json
import uuid

from .compute_modules import ComputeModule
from .common.utils import _remove_trailing_slash, Abstract
from .constants import DP_FOLDER
from data_pointer import DataPointer


class WorkflowEngine(Abstract):
    def __init__(self, workflow: Workflow, workspace: str,_key=None) -> None:
        super().__init__(_key)
        self.workflow = workflow
        self.workspace = _remove_trailing_slash(workspace)

    def _make_env(self):
        with open(f'{self.workspace}/env.json', 'w') as env_file:
            env = dict(
                PYTHONPATH=":".join([os.path.abspath(p) for p in sys.path]),
                PATH=os.environ.get('PATH', ""),
                execution_dir = os.getcwd()
            )
            json.dump(env, env_file,indent=4)

    def Run(self, targets: list[str]):
        raise NotImplementedError()

class SnakemakeEngine(WorkflowEngine):
    CONTAINER = '/home/tony/workspace/singularity/WF/snakemake/'
    def __init__(self, workflow: Workflow, workspace: str) -> None:
        super().__init__(workflow, workspace, self._abstract_initializer_key)

    def Run(self, targets: list[str]):
        module_names = set()
        T = '\t'
        ext = DataPointer.EXT
        WS_MOUNT = '/data'

        def _make_rule(m: ComputeModule):
            return "\n".join([
                f'rule {m.name}:',
                f'{T}input:',
                ",\n".join([f'{T}{T}"{DP_FOLDER}/{dtype}.{ext}"' for dtype in m._inputs]),
                f'{T}output:',
                ",\n".join([f'{T}{T}"{DP_FOLDER}/{dtype}.{ext}"' for dtype in m._outputs]),
                f'{T}shell:',
                f'{T}{T}"{m.GetEntryPoint()} {WS_MOUNT}"'
            ])

        def _make_targets():
            assert 'all' not in module_names, "[all] is not a valid module name, rename that folder"
            return "\n".join([
                f'rule all:',
                f'{T}input:',
                ",\n".join([f'{T}{T}"{DP_FOLDER}/{dtype}.{ext}"' for dtype in targets]),
            ])

        with open(f'{self.workspace}/snakefile', 'w') as sf:
            sm_config = ''
            sm_config += f'{_make_targets()}\n\n'
            for m in self.workflow.compute_modules:
                sm_config += f'{_make_rule(m)}\n\n'
            sf.write(sm_config)
        os.system(f'singularity run -B {self.workspace}:{WS_MOUNT} {self.CONTAINER}')

DefaultEngine = SnakemakeEngine

class Workflow:
    def __init__(self, compute_modules_path: str, engineType: type[WorkflowEngine] = DefaultEngine) -> None:

        compute_modules_path = _remove_trailing_slash(compute_modules_path)
        self.compute_modules: list[ComputeModule] = []
        for folder in os.listdir(compute_modules_path):
            module_path = f'{compute_modules_path}/{folder}'
            module = ComputeModule.LoadFromDisk(module_path)
            if module is not None:
                self.compute_modules.append(module)

        self.engineType = engineType

    def Run(self, workspace: str, targets: list[str]):
        workspace = os.path.abspath(workspace)
        if not os.path.isdir(workspace):
            os.mkdir(workspace)
        engine = self.engineType(self, workspace)
        engine.Run(targets)

