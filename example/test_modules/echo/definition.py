from metaworkflow.compute_module import Item, JobContext, JobResult

A = Item('test_text')
B = Item('test_echo')

def Procedure(context: JobContext) -> JobResult:
    out_file = context.output_folder.joinpath('echoed.txt')
    print(context.manifest)
    text_file = context.manifest[A]
    assert not isinstance(text_file, list)
    with open(text_file) as f:
        contents = "".join(l[:-1] for l in f.readlines())
        print(f'contents to echo: {contents}')

    return JobResult(
        exit_code = context.shell(f'echo "{contents}" > {out_file}'),
        manifest = {
            B: out_file
        },
    )

INPUTS = {A}
OUTPUTS = {B}
