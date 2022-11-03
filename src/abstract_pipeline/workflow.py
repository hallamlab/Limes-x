import os

from .workflow_engines import AbstractEngine, SnakemakeEngine as DefaultEngine
from .compute_modules import ComputeModule
from .common.utils import _remove_trailing_slash
from data_pointer import DataPointer

def _get_default_engine() -> AbstractEngine:
    return DefaultEngine()

class Workflow:
    def __init__(self, compute_modules_path: str, engine=_get_default_engine()) -> None:

        compute_modules_path = _remove_trailing_slash(compute_modules_path)
        self.compute_modules: list[ComputeModule] = []
        for folder in os.listdir(compute_modules_path):
            module_path = f'{compute_modules_path}/{folder}'
            module = ComputeModule.LoadFromDisk(module_path)
            if module is not None:
                self.compute_modules.append(module)

        self.engine = engine

    def Run(self, workspace: str, targets: list[str]):
        if not os.path.isdir(workspace):
            os.mkdir(workspace)
        self.engine.Run(workspace, targets)
