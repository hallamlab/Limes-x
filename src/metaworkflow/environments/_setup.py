import sys
from pathlib import Path
from dataclasses import dataclass

def ParseArgs():
    _paths: list = list(sys.argv[1:])
    assert len(_paths) == 3, f"{_paths}"
    MODULE_PATH, WORKSPACE, RELATIVE_OUTPUT_PATH = [Path(p) for p in _paths[:3]]
    # PYTHONPATH = _paths[3]
    # sys.path = list(set(sys.path + PYTHONPATH.split(':')))

    from metaworkflow.execution.modules import ComputeModule, JobContext
    # from metaworkflow.telemetry import ResourceMonitor

    CONTEXT = JobContext.LoadFromDisk(WORKSPACE.joinpath(RELATIVE_OUTPUT_PATH))
    # MODULE_PATH = '/'.join(os.path.realpath(__file__).split('/')[:-1])
    THIS_MODULE = ComputeModule._load(MODULE_PATH)

    @dataclass
    class ExecutionEssentials:
        module_path: Path
        module: ComputeModule
        workspace: Path
        relative_output_path: Path
        context: JobContext
        
    return ExecutionEssentials(
        module_path=MODULE_PATH,
        module=THIS_MODULE,
        workspace=WORKSPACE,
        relative_output_path=RELATIVE_OUTPUT_PATH,
        context=CONTEXT
    )