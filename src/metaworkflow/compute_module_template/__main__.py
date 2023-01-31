import sys, os
from pathlib import Path
import json
from datetime import datetime as dt

if __name__ == '__main__':

    from _receiver import THIS_MODULE, CONTEXT, WORKSPACE, RELATIVE_OUTPUT_PATH
    from metaworkflow.common.utils import LiveShell
    from metaworkflow.compute_module import JobResult

    cmd_history = []
    err_log, out_log = [], []
    def _shell(cmd: str):

        lines = cmd.split('\n')
        code = 0
        for line in lines:
            line = line.strip()
            if line == "": continue
            # timestamp = f"{dt.now().strftime('%d%b%Y-%H:%M:%S')}>"
            timestamp = f"{dt.now().strftime('%H:%M:%S')}>"
            prepped = f"{CONTEXT.shell_prefix} {line}"
            cmd_history.append(f"{timestamp} {line}")
            def _on_io(s: str, log: list):
                if s.endswith('\n'): s = s[:-1]
                log.append(f'{timestamp} {s}')

            code = LiveShell(
                prepped, echo_cmd=False,
                onOut=lambda s: _on_io(s, out_log),
                onErr=lambda s: _on_io(s, err_log),
            )
            if code != 0:
                return code

        return code

    CONTEXT.shell = _shell
    CONTEXT.output_folder = RELATIVE_OUTPUT_PATH

    os.chdir(WORKSPACE)
    # monitor = ResourceMonitor(relative_output_path)
    result = None
    try:
        result = THIS_MODULE._procedure(CONTEXT)
    finally:
        # res_log = monitor.Stop()
        if result is None:
            result = JobResult()
            result.error_message = f"procedure failed"
    # result.resource_log = res_log
    result.resource_log = []
    result.cmds = cmd_history
    result.out_log = out_log
    result.err_log = err_log

    result_path = RELATIVE_OUTPUT_PATH.joinpath('result.json')
    with open(result_path, 'w') as j:
        d = result.ToDict()
        json.dump(d, j, indent=4)
