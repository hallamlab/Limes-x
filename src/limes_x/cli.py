import os, sys
import argparse
import asyncio
from quart import Config
import yaml
from .utils import Version

def line():
    try:
        width = os.get_terminal_size().columns
    except:
        width = 32
    return "#"*width
    
class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))

def ui(args):
    from .ui.server import ui_app
    from .ui.config import ui_config
    from hypercorn.asyncio import serve

    asyncio.run(serve(ui_app, ui_config))

def outpost(args):
    from .outpost.launcher import EnsureServer, StartServer

    parser = ArgumentParser(
        prog = 'lx outpost',
    )
    parser.add_argument('-d', '--detach', action='store_true', default=False, help="detach and run in background", required=False)

    config = parser.parse_args(args)

    if config.detach:
        pid = EnsureServer()
        print(pid)
    else:
        StartServer()

def main_help(args=None):
    print(f"""\
Limes-x v{Version()}
https://github.com/hallamlab/Limes-x

Syntax: lx COMMAND [OPTIONS]

Where COMMAND is one of:
ui
outpost

for additional help, use:
lx COMMAND -h/--help
""")

def main():
    if len(sys.argv) <= 1:
        main_help()
        return

    { # switch
        "ui": ui,
        "outpost": outpost,
    }.get(
        sys.argv[1], 
        main_help # default
    )(sys.argv[2:])
