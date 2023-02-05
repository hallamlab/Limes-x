import sys, os
from pathlib import Path
import json
from datetime import datetime as dt

if __name__ == '__main__':
    SRC = os.path.abspath(Path(__file__).joinpath('../../..'))
    sys.path = list(set([SRC]+sys.path))
    from _setup import ParseArgs
    e = ParseArgs()
    MODULE_PATH, WORKSPACE, RELATIVE_OUTPUT_PATH, CONTEXT, THIS_MODULE = e.module_path, e.workspace, e.relative_output_path, e.context, e.module
    from limes_x.common.utils import LiveShell
    from limes_x.execution.modules import JobResult

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
    CONTEXT.lib = MODULE_PATH.joinpath('lib')
    CONTEXT.ref = MODULE_PATH.joinpath('ref')

    os.chdir(WORKSPACE)
    # monitor = ResourceMonitor(relative_output_path)
    result = None
    err = ""
    try:
        sys.path = list(set([str(CONTEXT.lib)] + sys.path))
        result = THIS_MODULE._procedure(CONTEXT)
    except Exception as e:
        err = str(e)
    finally:
        # res_log = monitor.Stop()
        if result is None:
            result = JobResult()
            result.manifest = {}
            result.error_message = err
    # result.resource_log = res_log
    result.resource_log = []
    result.commands = cmd_history
    result.out_log = out_log
    result.err_log = err_log

    for k, paths in list(result.manifest.items()):
        if isinstance(paths, list):
            relative = [Path(os.path.abspath(p)).relative_to(WORKSPACE) for p in paths]
        else:
            relative = Path(os.path.abspath(paths)).relative_to(WORKSPACE)
        result.manifest[k] = relative

    err_msg = None
    try:
        for ps in result.manifest.values():
            _break = False
            for p in ps if isinstance(ps, list) else [ps]:
                if os.path.exists(p): continue
                result.error_message = f'promised output at [{p}] missing'
                _break = True
                break
            if _break: break
    except Exception as e:
        err_msg = f'result manifest corrupted'

    result_path = RELATIVE_OUTPUT_PATH.joinpath('result.json')
    with open(result_path, 'w') as j:
        d = result.ToDict()
        json.dump(d, j, indent=4)
