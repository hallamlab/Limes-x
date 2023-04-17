from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

A = Item('a')
B = Item('b')

DEP = "image.sif"

def procedure(context: JobContext) -> JobResult:
    input_path = context.manifest[A]
    output_file_name = 'copied_file'
    context.shell(f"cp {input_path} {context.output_folder}/")
    return JobResult(
        manifest = {
            B: Path(output_file_name)
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(A, groupby=None)\
    .PromiseOutput(B)\
    .Requires({DEP})\
    .SuggestedResources(threads=1, memory_gb=4)\
    .SetHome(__file__, name=None)\
    .Build()
