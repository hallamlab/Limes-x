import os

from .common.utils import Abstract, AbstractClassException
from .compute_modules import ComputeModule
from data_pointer import DataPointer

class AbstractEngine(Abstract):
    def __init__(self, _key=None) -> None:
        super().__init__(_key)
        self.compute_modules: list[ComputeModule] = []
    
    def Load(self, compute_modules: list[ComputeModule]):
        pass

    def Run(self, workspace: str, targets: list[str]):
        pass

class SnakemakeEngine(AbstractEngine):
    CONTAINER = '/home/tony/workspace/singularity/WF/snakemake/'
    def __init__(self) -> None:
        super().__init__(self._abstract_initializer_key)
        
    def Run(self, workspace: str, targets: list[str]):
        with open(f'{workspace}/snakefile', 'w') as sf:
            with open('/home/tony/workspace/python/grad/gene_centric_analysis_pipeline/singularity/snakemake/cache/snakefile') as x:
                sf.write("".join(x.readlines()))

        os.system(f'singularity run -B {workspace}:/data {self.CONTAINER}')
