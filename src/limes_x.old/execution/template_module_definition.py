from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

A = Item('a')
B = Item('b')

DEPENDENCY = "image.sif"

def procedure(context: JobContext) -> JobResult:
    input_path = context.manifest[A]
    output_path = context.output_folder.joinpath('copied_file')
    context.shell(f"cp {input_path} {output_path}")
    return JobResult(
        manifest = {
            B: Path(output_path)
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(A, groupby=None)\
    .PromiseOutput(B)\
    .Requires({DEPENDENCY})\
    .SuggestedResources(threads=1, memory_gb=4)\
    .SetHome(__file__, name=None)\
    .Build()
