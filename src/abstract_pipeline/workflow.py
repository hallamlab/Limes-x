from __future__ import annotations
import sys, os
import json
import uuid

from .compute_modules import ComputeModule
from .common.utils import RemoveTrailingSlash, Abstract
from .constants import MANIFESTS_FOLDER, DATA_FOLDER
from data_pointer import Manifest, ManifestTemplate

class WorkflowEngine(Abstract):
    def __init__(self, workflow: Workflow, workspace: str,_key=None) -> None:
        super().__init__(_key)
        self.workflow = workflow
        self.workspace = RemoveTrailingSlash(workspace)

    def _make_env(self):
        with open(f'{self.workspace}/env.json', 'w') as env_file:
            env = dict(
                PYTHONPATH=":".join([os.path.abspath(p) for p in sys.path]),
                PATH=os.environ.get('PATH', ""),
                execution_dir = os.getcwd()
            )
            json.dump(env, env_file,indent=4)

    def Run(self, inputs: list[Manifest], targets: list[ManifestTemplate]):
        raise NotImplementedError()

class SnakemakeEngine(WorkflowEngine):
    CONDA_ENV = 'wf-snakemake'
    def __init__(self, workflow: Workflow, workspace: str) -> None:
        super().__init__(workflow, workspace, self._abstract_initializer_key)
        print('todo: pass through conda env location')

    def Run(self, inputs: list[Manifest], targets: list[ManifestTemplate]):
        T = '\t'

        def _make_rule(m: ComputeModule):
            return "\n".join([
                f'rule {m.name}:',
                f'{T}input:',
                ",\n".join([f'{T}{T}"{MANIFESTS_FOLDER}/{template.GenerateSaveName()}"' for template in m._inputs]),
                f'{T}output:',
                ",\n".join([f'{T}{T}"{MANIFESTS_FOLDER}/{template.GenerateSaveName()}"' for template in m._outputs]),
                f'{T}shell:',
                f'{T}{T}"{m.GetEntryPointCmd()}"'
            ])

        def _make_targets():
            module_names = set(m.name for m in self.workflow.compute_modules)
            assert 'all' not in module_names, "[all] is not a valid module name, rename that folder"
            return "\n".join([
                f'rule all:',
                f'{T}input:',
                ",\n".join([f'{T}{T}"{MANIFESTS_FOLDER}/{template.GenerateSaveName()}"' for template in targets]),
            ])

        for m in inputs:
            m.Save(f'{self.workspace}/{MANIFESTS_FOLDER}')

        with open(f'{self.workspace}/wf.smk', 'w') as sf:
            sm_config = ''
            sm_config += f'{_make_targets()}\n\n'
            for m in self.workflow.compute_modules:
                sm_config += f'{_make_rule(m)}\n\n'
            sf.write(sm_config)

        # todo: pass through additional snakemake params
        # todo: installer for conda envs
        conda_envs = '/'.join(os.environ['CONDA_PREFIX'].split('/')[:-1])
        snakemake_env = f'{conda_envs}/{self.CONDA_ENV}/bin'
        print(self.workspace)
        os.system(" && ".join([
            f'export PATH={snakemake_env}:$PATH',
            f'snakemake -d {self.workspace} -s {self.workspace}/wf.smk'
        ]))

DefaultEngine = SnakemakeEngine

class Workflow:
    def __init__(self, compute_modules_path: str, engineType: type[WorkflowEngine] = DefaultEngine) -> None:

        compute_modules_path = RemoveTrailingSlash(compute_modules_path)
        self.compute_modules: list[ComputeModule] = []
        for folder in os.listdir(compute_modules_path):
            module_path = f'{compute_modules_path}/{folder}'
            module = ComputeModule.LoadFromDisk(module_path)
            if module is not None:
                self.compute_modules.append(module)

        self.engineType = engineType

    def Run(self, workspace: str,
        inputs: list[Manifest], targets: list[ManifestTemplate]):

        # prepare folders
        workspace = os.path.abspath(workspace)
        man_dir = f'{workspace}/{MANIFESTS_FOLDER}'
        int_dir = f'{workspace}/{DATA_FOLDER}'
        for dir in [man_dir, int_dir]:
            if not os.path.exists(dir):
                os.makedirs(dir, exist_ok=True)

        # point inputs
        for d in inputs:
            d.Save(man_dir)

        # save environment
        abs_paths = lambda lst: [os.path.abspath(p) for p in lst]
        env = {
            'python_exe': sys.executable,
            'workspace': workspace,
            'PATH': abs_paths(str(os.environ.get('PATH', '')).split(':')),
            'PYTHONPATH': abs_paths(set(sys.path)),
        }
        with open(f'{workspace}/env.json', 'w') as f:
            json.dump(env, f, indent=4)

        # run
        engine = self.engineType(self, workspace)
        engine.Run(inputs, targets)

    def GetDataTypes(self):
        for m in self.compute_modules:
            m._inputs
