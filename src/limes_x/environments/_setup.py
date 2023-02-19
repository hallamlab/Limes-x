from re import VERBOSE
import sys, os
from pathlib import Path
from dataclasses import dataclass

def ParseArgs(python_path: list[str]|None=None):
    if python_path is not None:
        sys.path = python_path
    VERBOSE = sys.argv.pop() == "True"
    _paths: list = list(sys.argv[1:])
    assert len(_paths) == 3, f"bad receive {_paths}"
    MODULE_PATH, WORKSPACE, RELATIVE_OUTPUT_PATH = [Path(p) for p in _paths[:3]]

    from limes_x.execution.modules import ComputeModule, JobContext
    # from limes_x.telemetry import ResourceMonitor

    _here = os.getcwd()
    os.chdir(WORKSPACE) # to keep relative output path
    CONTEXT = JobContext.LoadFromDisk(RELATIVE_OUTPUT_PATH)
    THIS_MODULE = ComputeModule._load(MODULE_PATH)
    os.chdir(_here)

    @dataclass
    class ExecutionEssentials:
        module_path: Path
        module: ComputeModule
        workspace: Path
        relative_output_path: Path
        context: JobContext
        verbose: bool
        
    return ExecutionEssentials(
        module_path=MODULE_PATH,
        module=THIS_MODULE,
        workspace=WORKSPACE,
        relative_output_path=RELATIVE_OUTPUT_PATH,
        context=CONTEXT,
        verbose=VERBOSE,
    )