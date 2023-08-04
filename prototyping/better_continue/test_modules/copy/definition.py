from pathlib import Path
from limes_x import ExecutionContext
# from limes_x import ModuleBuilder, Item, JobContext, JobResult

SAMPLE = "sample"
COPIED = "copied test file"
EXECUTABLE = "example_image.sif"

FINAL = "final test file"

REQUIRES = {
    (COPIED, SAMPLE), # INPUT, grouped by SAMPLE
    COPIED,
    EXECUTABLE,
}

PRODUCES = {FINAL}

def Procedure(context: ExecutionContext) -> dict:
    M = context.manifest
    out = context.output_folder
    sample = M[SAMPLE]
    test_file = M[COPIED]
    copied: Path = out.joinpath(test_file.name)
    context.Shell(f"""\
        touch temp_file_in_temp_dir_for_{sample}
        cp {test_file} {copied}
    """)
    return {
        FINAL: copied
    }
