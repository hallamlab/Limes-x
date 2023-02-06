from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

A = Item('a')
B = Item('b')

def procedure(context: JobContext) -> JobResult:
    input_path = context.manifest[A]
    output_file_name = 'copied_file'
    return JobResult(
        exit_code = context.shell(f"cp {input_path} {context.output_folder}/"),
        manifest = {
            B: Path(output_file_name)
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(A, groupby=None)\
    .PromiseOutput(B)\
    .SetHome(__file__, name=None)\
    .Build()
