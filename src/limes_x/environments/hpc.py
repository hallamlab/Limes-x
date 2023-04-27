import json
import sys, os
from pathlib import Path
import time
import uuid
from datetime import datetime as dt
import random

if __name__ == '__main__':
    NEWLINE = '\n'

    TMP_NAME = sys.argv.pop()
    TMP = Path(os.environ.get(TMP_NAME, '/tmp'))
    LIB = sys.argv.pop()
    
    HPC_SPACE: Path|None = None
    while HPC_SPACE is None or HPC_SPACE.exists():
        HPC_SPACE = TMP.joinpath(f'limes_x-{uuid.uuid4().hex}')
    HPC_WS = HPC_SPACE.joinpath('workspace')
    HPC_LIB = HPC_SPACE.joinpath('lib'); os.makedirs(HPC_WS)
    HPC_REF = HPC_SPACE.joinpath('ref'); os.makedirs(HPC_REF)
    os.chdir(HPC_WS)
    os.system(f'mkdir -p {HPC_LIB} && tar -hxf {LIB} -C {HPC_LIB}')

    sys.path = list(set([str(HPC_LIB)]+sys.path))
    from _setup import ParseArgs
    e = ParseArgs(sys.path)
    MODULE_PATH, WORKSPACE, RELATIVE_OUTPUT_PATH, CONTEXT, THIS_MODULE, VERBOSE = e.module_path, e.workspace, e.relative_output_path, e.context, e.module, e.verbose
    os.makedirs(RELATIVE_OUTPUT_PATH)

    import limes_x.environments.local as env
    from limes_x.execution.modules import ComputeModule
    from limes_x.execution.comms import FileSyncedDictionary
    from limes_x.common.utils import LiveShell

    cmd_history = []
    err_log, out_log = [], []
    realtime_log = WORKSPACE.joinpath(RELATIVE_OUTPUT_PATH).joinpath('realtime.log')

    def _timestamp():
        return f"{dt.now().strftime('%H:%M:%S')}>"

    def _on_io(s: str, log: list, is_child: bool):
        if s.endswith('\n'): s = s[:-1]
        line = f'{_timestamp()} {s}'
        if not is_child: log.append(line)
        with open(realtime_log, 'a') as f:
            if not is_child: f.write(line+NEWLINE)

    def _shell(cmd: str, is_child: bool):
        cmd = cmd.replace("  ", "")
        lines = cmd.split('\n')
        ts = _timestamp()
        cmd_history.append(f"{ts}")
        for line in lines:
            line = line.strip()
            if line == "": continue
            cmd_history.append(f"{' '*(len(ts))}{line}")

        LiveShell(
            cmd, echo_cmd=False,
            onOut=lambda s: _on_io(s, out_log, is_child),
            onErr=lambda s: _on_io(s, err_log, is_child),
        )

    # get inputs
    for item, ps in CONTEXT.manifest.items():
        if not isinstance(ps, list): ps = [ps]
        for p in ps:
            if not isinstance(p, Path): continue
            toks = str(p).split('/')
            folder = '/'.join(toks[:-1])
            if not os.path.exists(folder): os.makedirs(folder, exist_ok=True)
            _shell(f"""\
                echo "---- getting input: {p}"
                cp -r -L {WORKSPACE.joinpath(p)} {folder}/
            """, is_child=False)
    _shell('''\
        echo "---- starting workspace:"
        ls | xargs -I {} sh -c "echo {}/ && ls -lh {}"
    ''', is_child=False)

    # get module src
    lib_name = env.__name__
    module_name = str(MODULE_PATH).split('/')[-1]
    _shell(f"""\
        echo "---- getting module src"
        mkdir -p {HPC_LIB}/{module_name}
        cp -r -L {MODULE_PATH}/{ComputeModule.LIB_FOLDER} {HPC_LIB}/{module_name}
        cd {HPC_WS} && ls -lh
    """, is_child=False)
    _shell(f"ls -lh {HPC_LIB}", is_child=False)

    # get requirements
    requirements = [str(CONTEXT.params.reference_folder.joinpath(req)) for req in THIS_MODULE.requirements]
    req_ok = True
    for req_path in requirements:
        found = False
        for cmd, req in [
            # (f"cd {HPC_REF} && pigz -dc {req_path}.tar.gz | tar -", f"{req_path}.tar.gz"),
            (f"cd {HPC_REF} && pigz -dc {req_path}.lx.tgz | tar -", f"{req_path}.lx.tgz"),
            (f"cp -r {req_path} {HPC_REF}", req_path),
        ]:
            if not os.path.exists(req): continue
            found = True
            _shell(f"""\
                echo "---- getting requirement: {req}"
                {cmd}
            """, is_child=False)
            break

        if not found:
            req_ok = False
            _shell(f'echo "ERROR: requirement [{req_path}] missing"', is_child=False)
            break
    _shell(f"ls -lh {HPC_REF}", is_child=False)
    CONTEXT.params.reference_folder = HPC_REF
    CONTEXT.ref = HPC_LIB
    CONTEXT.Save(HPC_WS)

    # remove myself from list of io jobs
    with FileSyncedDictionary(WORKSPACE) as com:
        com.RemoveIoTask(CONTEXT.job_id)

    # run step if @req met
    if req_ok:
        _shell("echo running...", is_child=False)
        _shell(f"""\
            python {env.__file__} {HPC_LIB}/{module_name} {HPC_WS} {RELATIVE_OUTPUT_PATH} {True}\
        """, is_child=True)

        # gather results
        BL = {
            'context.json',
            'result.json',
            'realtime.log'
        }
        LOCAL_OUT_PATH = Path(WORKSPACE).joinpath(RELATIVE_OUTPUT_PATH)
        for out in os.listdir(RELATIVE_OUTPUT_PATH):
            if out in BL: continue
            _shell(f"""\
                echo "---- copying back result: {out}"
                cd {RELATIVE_OUTPUT_PATH}
                cp -r {out} {LOCAL_OUT_PATH.joinpath(out)}
            """, is_child=False)
        _shell("""\
            echo "---- final workspace:"
            ls | xargs -I {} sh -c "echo {}/ && ls -lh {}"
            echo "---- done!"
        """, is_child=False)
    else:
        _shell('echo "terminating due to missing requirement', is_child=False)

    result_json = 'result.json'
    result_path = RELATIVE_OUTPUT_PATH.joinpath(result_json)
    def _get_result_json():
        if result_path.exists():
            with open(result_path) as j:
                try:
                    return json.load(j)
                except json.JSONDecodeError:
                    return {}
        else:
            return {}

    res = _get_result_json()
    res['hpc-wrapper_commands'] = cmd_history
    res['hpc-wrapper_out'] = out_log
    res['hpc-wrapper_err'] = err_log
    with open(WORKSPACE.joinpath(RELATIVE_OUTPUT_PATH).joinpath(result_json), 'w') as outj:
        json.dump(res, outj, indent=4)
