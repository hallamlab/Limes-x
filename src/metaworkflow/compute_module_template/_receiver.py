import sys
from pathlib import Path

_paths: list = list(sys.argv[1:])
assert len(_paths) == 4, f"{_paths}"
MODULE_PATH, WORKSPACE, RELATIVE_OUTPUT_PATH = [Path(p) for p in _paths[:3]]
PYTHONPATH = str(_paths[3])
sys.path = list(set(sys.path + PYTHONPATH.split(':')))

from metaworkflow.compute_module import ComputeModule, JobContext
# from metaworkflow.telemetry import ResourceMonitor

CONTEXT = JobContext.LoadFromDisk(WORKSPACE.joinpath(RELATIVE_OUTPUT_PATH))
# MODULE_PATH = '/'.join(os.path.realpath(__file__).split('/')[:-1])
THIS_MODULE = ComputeModule._load(MODULE_PATH)
