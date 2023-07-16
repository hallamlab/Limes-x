import os, sys
import argparse
import asyncio
from pathlib import Path

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

def outpost(raw_args):
    from .outpost.launcher import EnsureServer, StartServer
    from .outpost.models import Config

    parser = ArgumentParser(
        prog = 'lx outpost',
    )
    parser.add_argument('-c', '--config', metavar='PATH', help="config.yml", required=True)
    parser.add_argument('-d', '--detach', action='store_true', default=False, help="detach and run in background", required=False)

    args = parser.parse_args(raw_args)

    config_file = Path(args.config)
    config = Config.Load(config_file)

    if args.detach:
        if 0 == os.fork(): # fork returns 0 in child and pid of child in parent
            StartServer(config)
    else:
        StartServer(config)

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
