from __future__ import annotations
import os, sys
import time
import json
from pathlib import Path
from typing import Callable
import inspect
from .modules import ComputeModule, JobContext, JobResult, Params, Item
from .instances import JobInstance
from ..common.utils import LiveShell

class Job:
    instance: JobInstance
    context: JobContext
    run_command: str
    workspace: Path

    def __init__(self, instance: JobInstance, workspace: Path, params: Params) -> None:
        self.instance = instance
        self.workspace = workspace

        c = JobContext()
        c.job_id = self.instance.GetID()
        c.output_folder = Path(f"{self.instance.step.name}--{self.instance.GetID()}")
        c.params = params.Copy()
        c.manifest = dict((Item(k), [ii.path for ii in v] if isinstance(v, list) else v.path) for k, v in self.instance.inputs.items())
        c.Save(workspace)
        self.context = c

    def Shell(self, cmd: str):
        err_log = []
        code=LiveShell(cmd, echo_cmd=False, onErr=lambda s: err_log.append(s))
        return code==0, "".join(err_log)

ExecutionHandler = Callable[[Job], tuple[bool, str]]
class Executor:
    def __init__(self, execute_procedure: ExecutionHandler|None=None) -> None:
        self.execute_procedure: ExecutionHandler = execute_procedure if execute_procedure is not None else lambda j: j.Shell(j.run_command)

    def Run(self, instance: JobInstance, workspace: Path, params: Params) -> JobResult:
        job = Job(
            instance = instance,
            workspace = workspace,
            params = params,
        )

        from . import reciever
        entry_point = Path(os.path.abspath(inspect.getfile(reciever)))
        job.run_command = f"""\
            PYTHONPATH={':'.join(os.path.abspath(p) for p in sys.path)}
            python {entry_point} {job.instance.step.location} {workspace} {job.context.output_folder}
        """[:-1].replace("  ", "")
        success, msg = self.execute_procedure(job)

        return self._compile_result(job, success, msg)

    def _compile_result(self, job: Job, success: bool, msg: str):
        if not success:
            return self._make_failed_result(job.instance, msg)
        else:
            return self._get_result(job.context, job.instance)

    def _get_result(self, context: JobContext, job: JobInstance) -> JobResult:
        result_json = context.output_folder.joinpath('result.json')
        if not os.path.exists(result_json):
            w = context.params.file_system_wait_sec
            if w > 0:
                print(f"waiting {w} sec. for {job.step.name}:{job.GetID()}")
                time.sleep(w)

        if os.path.exists(result_json):
            err_msg = None
            try:
                with open(result_json) as j:
                    r = JobResult.FromDict(json.load(j))
                    r.made_by = job.GetID()

                    for ps in r.manifest.values():
                        _break = False
                        for p in ps if isinstance(ps, list) else [ps]:
                            if os.path.exists(p): continue
                            r.error_message = f'promised output at [{p}] missing'
                            _break = True
                            break
                        if _break: break
                    return r
            except Exception as e:
                err_msg = f'result manifest corrupted'
        else:
            WS = '{workspace}'
            err_msg = f'missing result manifest at [{WS}/{result_json}]'

        r = JobResult()
        r.made_by = job.GetID()
        if not err_msg is None:
            if err_msg[-1] == '\n': err_msg = err_msg[:-1]
            r.error_message = err_msg
        return r

    def _make_failed_result(self, job: JobInstance, msg: str):
        r = JobResult()
        r.made_by = job.GetID()
        r.error_message = f"executor failed:\n{msg}"
        return r

class CloudExecutor(Executor):
    def __init__(self, execute_procedure: ExecutionHandler|None=None) -> None:
        super().__init__(execute_procedure)

    def Run(self, instance: JobInstance, workspace: Path, params: Params) -> JobResult:
        job = Job(
            instance = instance,
            workspace = workspace,
            params = params,
        )

        from . import reciever
        entry_point = Path(os.path.abspath(inspect.getfile(reciever)))
        job.run_command = f"""\
            PYTHONPATH={':'.join(os.path.abspath(p) for p in sys.path)}
            python {entry_point} {job.instance.step.location} {workspace} {job.context.output_folder}
        """[:-1].replace("  ", "")
        success, msg = self.execute_procedure(job)

        return self._compile_result(job, success, msg)
