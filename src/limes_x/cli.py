import os, sys
import argparse
from pathlib import Path

from .execution.modules import ComputeModule

HERE = Path("/".join(os.path.realpath(__file__).split('/')[:-1]))
NAME = "Limes-x"
with open(HERE.joinpath("version.txt")) as f:
    VERSION = f.read()

HEADER = f"""\
{NAME}
v{VERSION}
"""
CMD = "lx"

class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))

def fprint(x):
    return print(f"{x}".replace("  ", ""))

def print_header():
    fprint("#"*os.get_terminal_size().columns)
    fprint(HEADER)

def help():
    fprint(f"""\
        {HEADER}
        Syntax: lx COMMAND [OPTIONS]

        Where COMMAND is one of:
        setup

        for additional help, use:
        {CMD} COMMAND -h
    """)

def _flatten(lst):
    return [x for g in lst for x in g] if lst is not None else []

def setup(args):
    parser = ArgumentParser(prog=f'{CMD} setup')

    parser.add_argument(
        '--modules', "-m", metavar='PATH',
        action='append', nargs='+',
        help="location(s) of compute modules", required=True
    )
    parser.add_argument('--output', "-o", metavar='PATH', help="where to setup dependencies", required=True)
    parser.add_argument(
        '--blacklist', "-b",
        action='append', nargs='+', default=[],
        metavar='PATH', help="names of modules to skip"
    )
    pargs = parser.parse_args(args)

    ref = Path(pargs.output)
    if not ref.exists():
        os.makedirs(ref, exist_ok=True)

    bl = {str(n) for n in _flatten(pargs.blacklist)}

    module_folders = [Path(p) for p in _flatten(pargs.modules)]
    modules = []
    skipped = 0
    for p in module_folders:
        assert p.exists(), f"can't find {p}, try full path?"
        all_mods = ComputeModule.LoadSet(p)
        to_add = []
        for m in all_mods:
            if m.name in bl:
                skipped += 1
                continue
            to_add.append(m)
        modules += to_add

    modules = sorted(modules, key=lambda m: m.name)

    print(f"setting up {len(modules)} modules")
    print(f"skipped {skipped}")
    for m in modules:
        print(f" - {m.name}")

    print()
    for i, m in enumerate(modules):
        fprint(f"{i+1} of {len(modules)}")
        m.Setup(ref, "singularity") # only sing. for now
        if i >= 1: break

    print("done")

# def local(args):
#     sys.argv = ["python"]+args
#     from .execution.executor_presets import local

# def slurm(args):
#     sys.argv = ["python"]+args
#     from .execution.executor_presets import slurm

# def pbs(args):
#     sys.argv = ["python"]+args
#     from .execution.executor_presets import pbs

def main():
    if len(sys.argv) <= 1:
        help()
        return
    
    { # switch
        "setup": setup,
        # "local": local,
        # "slurm": slurm,
        # "pbs": pbs,
    }.get(
        sys.argv[1], 
        lambda args: help # default
    )(sys.argv[2:])