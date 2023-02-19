import json
import sys, os
from pathlib import Path
import uuid
from datetime import datetime as dt

if __name__ == '__main__':
    NEWLINE = '\n'

    NO_ZIP = sys.argv.pop().split(":")
    TMP_NAME = sys.argv.pop()
    TMP = Path(os.environ.get(TMP_NAME, '/tmp'))
    LIB = sys.argv.pop()
    # TMP = Path("/home/phyberos/project-rpp/gene_centric_analysis_pipeline/scratch/cloud_compute/cache/tmp")
    
    CLOUD_SPACE: Path|None = None
    while CLOUD_SPACE is None or CLOUD_SPACE.exists():
        CLOUD_SPACE = TMP.joinpath(f'limes_x-{uuid.uuid4().hex}')
    CLOUD_WS = CLOUD_SPACE.joinpath('workspace')
    CLOUD_LIB = CLOUD_SPACE.joinpath('lib'); os.makedirs(CLOUD_WS)
    CLOUD_REF = CLOUD_SPACE.joinpath('ref'); os.makedirs(CLOUD_REF)
    os.chdir(CLOUD_WS)
    os.system(f'mkdir -p {CLOUD_LIB} && tar -hxf {LIB} -C {CLOUD_LIB}')

    sys.path = list(set([str(CLOUD_LIB)]+sys.path))
    from _setup import ParseArgs
    e = ParseArgs(sys.path)
    MODULE_PATH, WORKSPACE, RELATIVE_OUTPUT_PATH, CONTEXT, THIS_MODULE, VERBOSE = e.module_path, e.workspace, e.relative_output_path, e.context, e.module, e.verbose
    os.makedirs(RELATIVE_OUTPUT_PATH)

    unzipped = set()
    for item, ps in CONTEXT.manifest.items():
        if not isinstance(ps, list): ps = [ps]
        for p in ps:
            if not isinstance(p, Path): continue
            toks = str(p).split('/')
            folder = toks[0]
            output = '/'.join(toks[:2])
            os.system(f'echo "{p}" >> {WORKSPACE}/x')
            os.system(f'echo "{toks}" >> {WORKSPACE}/x')
            os.system(f'echo "{folder}" >> {WORKSPACE}/x')
            if not os.path.exists(folder): os.makedirs(folder)
            if output in unzipped: continue
            os.system(f"cd {folder} && tar -hxf {WORKSPACE.joinpath(f'{output}.tgz')}")
            unzipped.add(output)

    import limes_x.environments.local as env
    from limes_x.execution.modules import ComputeModule
    from limes_x.common.utils import LiveShell

    cmd_history = []
    err_log, out_log = [], []
    def _shell(cmd: str, is_child=True):
        realtime_log = WORKSPACE.joinpath(RELATIVE_OUTPUT_PATH).joinpath('realtime.log')
        cmd = cmd.replace("  ", "")
        lines = cmd.split('\n')
        for line in lines:
            line = line.strip()
            if line == "": continue
            cmd_history.append(line)
            timestamp = f"{dt.now().strftime('%H:%M:%S')}>"
            def _on_io(s: str, log: list):
                if s.endswith('\n'): s = s[:-1]
                line = f'{timestamp} {s}'
                if not is_child: log.append(line)
                with open(realtime_log, 'a') as f:
                    f.write(line+NEWLINE) if not is_child else f.write(s+'\n')
        LiveShell(
            cmd, echo_cmd=False,
            onOut=lambda s: _on_io(s, out_log),
            onErr=lambda s: _on_io(s, err_log),
        )

    # print(sys.argv, limes_x.__name__)
    lib_name = env.__name__
    module_name = str(MODULE_PATH).split('/')[-1]
    _shell(f"""\
        echo $(date) "setting up env"
        mkdir -p {CLOUD_LIB}/{module_name}
        cp -r {MODULE_PATH}/{ComputeModule.LIB_FOLDER} {CLOUD_LIB}/{module_name}
        cd {CLOUD_WS}
        ls -lh
    """)

    requirements = [str(CONTEXT.params.reference_folder.joinpath(req)) for req in THIS_MODULE.requirements]
    zreqs, cpreqs = [], []
    for r in requirements:
        if any(r.endswith(e) for e in NO_ZIP):
            cpreqs.append(r)
        else:
            zreqs.append(r)
    _shell(f"""\
        echo $(date) "setting up requirements"
        cd {CLOUD_REF}
        {NEWLINE.join(f"tar -hxf {req}.tgz" for req in zreqs)}
        {NEWLINE.join(f"cp {req} ./" for req in cpreqs)}
        ls -lh
    """)
    CONTEXT.params.reference_folder = CLOUD_REF
    CONTEXT.lib = CLOUD_LIB
    CONTEXT.Save(CLOUD_WS)

    _shell("echo $(date) running...")
    _shell(f"""\
        python {env.__file__} {CLOUD_LIB}/{module_name} {CLOUD_WS} {RELATIVE_OUTPUT_PATH} {True}\
    """, is_child=True)
    BL = {
        'context.json',
        'result.json',
        'realtime.log'
    }
    outs = [f for f in os.listdir(RELATIVE_OUTPUT_PATH) if f not in BL]
    _shell(f"""\
        echo $(date) "gathering results"
        cd {RELATIVE_OUTPUT_PATH}
        {NEWLINE.join(f"tar -cf - {f} | pigz -5 -p {CONTEXT.params.threads} >{Path(WORKSPACE).joinpath(RELATIVE_OUTPUT_PATH)}/{f}.tgz" for f in outs)}
        echo $(date) "done"
        du -sh *
    """)

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
    res['cloud-wrapper_commands'] = cmd_history
    res['cloud-wrapper_out'] = out_log
    res['cloud-wrapper_err'] = err_log
    with open(WORKSPACE.joinpath(RELATIVE_OUTPUT_PATH).joinpath(result_json), 'w') as outj:
        json.dump(res, outj, indent=4)
