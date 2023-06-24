from pathlib import Path
from limes_x import ExecutionContext
# from limes_x import ModuleBuilder, Item, JobContext, JobResult

# SAMPLE = "sample"
# INPUT = "test file"
# EXECUTABLE = "example_image.sif"

_ins, _outs = Path(__file__).parent.name.split("-")
_in_set = {c for c in _ins}
_out_set = {_outs} if len(_outs)>=5 else {c for c in _outs}

REQUIRES = _in_set
PRODUCES = _out_set

def Procedure(context: ExecutionContext) -> dict:
    M = context.manifest
    out = context.output_folder
    cmd = []
    rep_in = None
    for i in REQUIRES:
        p = M[i]
        cmd.append(f"cat {p}")
        rep_in= p
    assert rep_in is not None
    outs = {}
    for o in PRODUCES:
        p = out.joinpath(o)
        outs[o] = p
        cmd.append(f"cp {rep_in} {p}")
    context.Shell("\n".join(cmd))
    return outs
