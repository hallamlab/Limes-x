from abstract_pipeline.compute_module import Item, JobContext, JobResult

A = Item('a')
B = Item('b')

def Procedure(context: JobContext) -> JobResult:
    dummy_out = context.output_folder.joinpath('dummy_out.file')
    return JobResult(
        exit_code = context.shell(f"touch {dummy_out}"),
        manifest = {
            B: dummy_out
        },
    )

INPUTS = {A}
OUTPUTS = {B}
