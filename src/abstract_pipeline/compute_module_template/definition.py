import os
from abstract_pipeline.compute_module import Item, RunContext, RunResult

A = Item.Get('a')
B = Item.Get('b')

def Procedure(context: RunContext) -> RunResult:
    print(context.__dict__)
    dummy_out = context.output_folder.joinpath('dummy_out.file')
    return RunResult(
        exit_code = context.shell(f"touch {dummy_out}"),
        manifest = {
            B: dummy_out
        },
    )

INPUTS = {A}
OUTPUTS = {B}
