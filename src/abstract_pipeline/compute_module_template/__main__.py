import sys, os
from pathlib import Path
import json
    
if __name__ == '__main__':
    _paths: list = list(sys.argv)
    assert len(_paths) == 3
    WORKSPACE, relative_output_path = [Path(p) for p in _paths[1:3]]

    ENV: dict = {}
    with open(WORKSPACE.joinpath(relative_output_path).joinpath('env.json')) as j:
        ENV = json.load(j)

    sys.path=ENV.get('PYTHONPATH', sys.path)

    from abstract_pipeline.compute_module import RunContext, ComputeModule
    from abstract_pipeline.common.utils import LiveShell

    MODULE_PATH = '/'.join(os.path.realpath(__file__).split('/')[:-1])
    module = ComputeModule.LoadFromDisk(MODULE_PATH)

    context = RunContext.FromDict(ENV.get('context', {}))
    context.shell = lambda c: LiveShell(f"{ENV.get('shell_prefix', '')} {c}".strip())
    print('s')
    os.chdir(WORKSPACE)
    context.output_folder = relative_output_path
    result = module._procedure(context)

    result_path = context.output_folder.joinpath('result.json')
    with open(result_path, 'w') as j:
        json.dump(result.ToDict(), j, indent=4)
