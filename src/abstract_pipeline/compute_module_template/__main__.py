import sys, os
from pathlib import Path
import json

from abstract_pipeline.compute_module import ComputeModule, JobResult
from abstract_pipeline.executors import JobEnv
from abstract_pipeline.common.utils import LiveShell
from abstract_pipeline.telemetry import ResourceMonitor

if __name__ == '__main__':
    _paths: list = list(sys.argv)
    assert len(_paths) == 3
    WORKSPACE, relative_output_path = [Path(p) for p in _paths[1:3]]
    ENV = JobEnv.FromWorkspace(WORKSPACE.joinpath(relative_output_path))
    MODULE_PATH = '/'.join(os.path.realpath(__file__).split('/')[:-1])
    THIS_MODULE = ComputeModule._load(MODULE_PATH)

    context = ENV.context
    cmds = []
    err_log, out_log = [], []
    def _shell(cmd: str):
        prepped = f"{ENV.shell_prefix} {cmd}".strip()
        cmds.append(prepped)
        echo = context.params.echo_stdout
        def _on_io(s: str, log: list, pre: str):
            if echo: print(f'{pre}:{context.job_id}: {s}')
            log.append(s)

        return LiveShell(
            prepped, echo_cmd=echo,
            onOut=lambda s: _on_io(s, out_log, "I"),
            onErr=lambda s: _on_io(s, err_log, "E"),
        )
    context.shell = _shell
    context.output_folder = relative_output_path

    os.chdir(WORKSPACE)
    monitor = ResourceMonitor(relative_output_path)
    result = None
    try:
        result = THIS_MODULE._procedure(context)
    finally:
        res_log = monitor.Stop()
        if result is None:
            result = JobResult()
            result.error_message = f"procedure failed"
    result.resource_log = res_log
    result.cmds = cmds
    result.out_log = out_log
    result.err_log = err_log

    result_path = relative_output_path.joinpath('result.json')
    with open(result_path, 'w') as j:
        d = result.ToDict()
        json.dump(d, j, indent=4)
