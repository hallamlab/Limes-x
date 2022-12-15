from abstract_pipeline.compute_module import Item, RunContext, RunResult

A = Item('a')
B = Item('b')

def Procedure(context: RunContext) -> RunResult:
    dummy_out = context.output_folder.joinpath('dummy_out.file')
    return RunResult(
        exit_code = context.shell(f"touch {dummy_out}"),
        manifest = {
            B: dummy_out
        },
    )

INPUTS = {A}
OUTPUTS = {B}
