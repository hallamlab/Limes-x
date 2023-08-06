from pathlib import Path
from limes_x.compute_module import ExecutionContext, CreateTransform

ME = CreateTransform(__file__)

READS = ME.AddRequirement({"a"})
ME.AddRequirement({"sif", "example_image"})
B = ME.AddProduct({"b"})

def Procedure(context: ExecutionContext) -> dict:
    M = context.manifest
    out = context.output_folder
    a = M[READS]
    copied: Path = out.joinpath(a.name)
    context.Shell(f"""\
        cp {a} {copied}
    """)
    return {
        B: copied
    }
