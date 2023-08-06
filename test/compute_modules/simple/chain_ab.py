from pathlib import Path
from limes_x.compute_module import ExecutionContext, CreateTransform

ME = CreateTransform(__file__)

A = ME.AddRequirement({"a"})
B = ME.AddProduct({"b"})

def Procedure(context: ExecutionContext) -> dict:
    M = context.manifest
    out = context.output_folder
    a = M[A]
    copied: Path = out.joinpath(a.name)
    context.Shell(f"""\
        cp {a} {copied}
    """)
    return {
        B: copied
    }
