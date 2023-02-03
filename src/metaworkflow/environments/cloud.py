import sys, os
from pathlib import Path
import uuid

MODULE_TEMPLATE_PATH = Path("/".join(sys.argv[3].split("/")[:-1]))
TMP_NAME = sys.argv[2]
original_path = sys.path.copy()
sys.path = list(set([str(MODULE_TEMPLATE_PATH)] + sys.path))
sys.argv = sys.argv[3:]
from _receiver import MODULE_PATH, CONTEXT, WORKSPACE, RELATIVE_OUTPUT_PATH, PYTHONPATH # type:ignore
from metaworkflow.execution.executors import JobContext

_CONTEXT: JobContext = CONTEXT
_WORKSPACE: Path = WORKSPACE
_RELATIVE_OUTPUT_PATH: Path = RELATIVE_OUTPUT_PATH

# TMP = Path(os.environ.get(TMP_NAME, '/tmp'))
# TMP = Path("/home/phyberos/project-rpp/gene_centric_analysis_pipeline/scratch/cloud_compute/cache/tmp")
TMP = Path("/home/tony/workspace/python/grad/gene_centric_analysis_pipeline/scratch/cloud_compute/cache/tmp")
CLOUD_SPACE: Path|None = None
while CLOUD_SPACE is None or CLOUD_SPACE.exists():
    CLOUD_SPACE = TMP.joinpath(f'{uuid.uuid4().hex}')
CLOUD_WS = CLOUD_SPACE.joinpath('workspace')
os.makedirs(CLOUD_WS)
os.chdir(CLOUD_WS)

# tars are of the entire run folder (like ws/echo--hexhex)
def _decompress(src: Path):
    os.system(f"tar -xf {src} -C .")

for item, ps in _CONTEXT.manifest.items():
    if not isinstance(ps, list): ps = [ps]
    for p in ps:
        folder = "/".join(str(p).split('/')[:-1])
        if os.path.exists(folder): continue
        _decompress(_WORKSPACE.joinpath(f"{folder}.tar.gz"))

import metaworkflow
lib_src = "/".join(metaworkflow.__file__.split("/")[:-1])

# print(sys.argv, metaworkflow.__name__)
lib_name = metaworkflow.__name__
lib_dir = CLOUD_SPACE.joinpath(f"lib/")
module_name = str(MODULE_PATH).split('/')[-1]
os.makedirs(_RELATIVE_OUTPUT_PATH)
os.makedirs(lib_dir)
os.system(f"""\
    cd {CLOUD_SPACE}
    cp -r {os.path.realpath(lib_src)} {lib_dir}/
    cp -r {os.path.realpath(MODULE_PATH)} {lib_dir}/
    rm -r {lib_dir}/*/__pycache__
    cd {CLOUD_WS}
    cp {_WORKSPACE.joinpath(_RELATIVE_OUTPUT_PATH)}/*.json {_RELATIVE_OUTPUT_PATH}/
    python {lib_dir}/{lib_name}/compute_module_template {lib_dir}/{module_name} {CLOUD_WS} {_RELATIVE_OUTPUT_PATH} {PYTHONPATH} \
        && tar -cf - {_RELATIVE_OUTPUT_PATH} | pigz -5 -p {_CONTEXT.params.threads} >{Path(WORKSPACE).joinpath(_RELATIVE_OUTPUT_PATH)}.tar.gz \
""".replace("  ", ""))
