from pathlib import Path
from limes_x.compute_module import ExecutionContext, CreateTransform

ME = CreateTransform(__file__)

B = ME.AddRequirement({"b"})
C = ME.AddProduct({"c"})

def Procedure(context: ExecutionContext) -> dict:
    M = context.manifest
    out = context.output_folder
    a = M[B]
    copied: Path = out.joinpath(a.name)
    context.Shell(f"""\
        cp {a} {copied}
    """)
    return {
        C: copied
    }
