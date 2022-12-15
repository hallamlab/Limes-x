import os, sys
from pathlib import Path
import json
from .compute_module import ComputeModule, RunContext, RunResult
from .common.utils import LiveShell

class Executor:
    def Run(self, compute_module: ComputeModule, context: RunContext) -> RunResult:
        raise NotImplementedError

class CondaExecutor(Executor):
    def __init__(self, env_name: str) -> None:
        self.env_name = env_name

    def Run(self, compute_module: ComputeModule, context: RunContext) -> RunResult:
        out = context.output_folder
        os.makedirs(out, exist_ok=True)
        env = {
            "shell_prefix": f"conda run --no-capture-output -n {self.env_name}",
            "context": context.ToDict(),
            "PYTHONPATH": [str(p) for p in sys.path],
        }
        with open(out.joinpath('env.json'), 'w') as j:
            json.dump(env, j, indent=4)

        module_cmd = compute_module.GenerateStaticRunCommand(Path(os.getcwd()), context.output_folder)

        code = LiveShell(module_cmd, echo_cmd=False)
        if code != 0:
            return RunResult(exit_code=code)
        
        result_json = context.output_folder.joinpath('result.json')
        try:
            with open(result_json) as j:
                return RunResult.FromDict(json.load(j))
        except FileNotFoundError:
            print(f'missing result manifest at [{result_json}]')
            return RunResult(exit_code=1)
