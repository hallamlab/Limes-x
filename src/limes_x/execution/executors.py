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

    def __init__(self, instance: JobInstance, workspace: Path, params: Params, _save=True) -> None:
        self.instance = instance
        self.workspace = workspace

        c = JobContext()
        c.job_id = self.instance.GetID()
        c.output_folder = Path(f"{self.instance.step.name}--{self.instance.GetID()}")
        c.params = params.Copy()
        c.lib = instance.step.location.joinpath(ComputeModule.LIB_FOLDER)
        c.manifest = dict((Item(k), [ii.value for ii in v] if isinstance(v, list) else v.value) for k, v in self.instance.inputs.items())
        if _save: c.Save(workspace)
        self.context = c

    def Shell(self, cmd: str):
        err_log = []
        code=LiveShell(cmd, echo_cmd=False, onErr=lambda s: err_log.append(s))
        return code==0, "".join(err_log)

ExecutionHandler = Callable[[Job], tuple[bool, str]]
SetupHandler = Callable[[list[ComputeModule], Path, Params], None]
class Executor:
    def __init__(self, execute_procedure: ExecutionHandler|None=None, prepare_run: SetupHandler|None=None) -> None:
        self._execute_procedure: ExecutionHandler = execute_procedure if execute_procedure is not None else lambda j: j.Shell(j.run_command)
        self._prepare_run = (lambda x, y, z: None) if prepare_run is None else prepare_run

    def PrepareRun(self, modules: list[ComputeModule], inputs_folder: Path, params: Params):
        self._prepare_run(modules, inputs_folder, params)

    def _overload_params(self, job: Job):
        step = job.instance.step
        params = job.context.params
        if step.threads is not None: params.threads = step.threads
        if step.memory_gb is not None: params.mem_gb = step.memory_gb
        return job

    def _make_job(self, instance: JobInstance, workspace: Path, params: Params, _save=True):
        job = self._overload_params(Job(
            instance = instance,
            workspace = workspace,
            params = params,
            _save = _save,
        ))
        return job

    def Run(self, instance: JobInstance, workspace: Path, params: Params) -> JobResult:
        job = self._make_job(instance, workspace, params)

        from ..environments import local
        entry_point = Path(os.path.abspath(inspect.getfile(local)))
        job.run_command = f"""\
            PYTHONPATH={':'.join(os.path.abspath(p) for p in sys.path)}
            python {entry_point} {job.instance.step.location} {workspace} {job.context.output_folder}
        """[:-1].replace("  ", "")
        success, msg = self._execute_procedure(job)

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
                    if r.manifest is None:
                        r.error_message = "no output created"
                        r.manifest = {}
                    return r
            except Exception as e:
                err_msg = f'result manifest corrupted'
        else:
            WS = '{workspace}'
            err_msg = f'missing result manifest at [{WS}/{result_json}]'

        r = JobResult()
        r.made_by = job.GetID()
        if err_msg is not None:
            if err_msg[-1] == '\n': err_msg = err_msg[:-1]
            r.error_message = err_msg
        return r

    def _make_failed_result(self, job: JobInstance, msg: str):
        r = JobResult()
        r.made_by = job.GetID()
        r.error_message = f"executor failed:\n{msg}"
        return r

class CloudExecutor(Executor):
    _EXT = 'tgz'
    _SRC_FOLDER_NAME = 'limesx_src'
    _NO_ZIP = ['tgz', 'tar.gz', 'sif']
    def __init__(self, zipped_inputs: Path|None=None, execute_procedure: ExecutionHandler | None = None, prerun: Callable[[Path], None] | None = None) -> None:
        def _prepare_run(modules: list[ComputeModule], inputs_dir: Path, params: Params):
            _shell = lambda cmd: LiveShell(cmd=cmd.replace('  ', ''), echo_cmd=False)
            HERE = os.getcwd()
            NEWL = '\n'
            EXT = self._EXT
            THREADS = params.logistic_threads if params.logistic_threads is not None else params.threads

            ## requirements ##
            if os.path.exists(params.reference_folder):
                requirements = {req for g in [m.requirements for m in modules] for req in g if not any(req.endswith(e) for e in self._NO_ZIP)}
                for req in list(requirements):
                    if os.path.exists(f'{params.reference_folder}/{req}.{EXT}'):
                        print(f'using cached {req}.{EXT}')
                        requirements.remove(req)
                    else:
                        print(f'zipping {req} for cloud')
                        _shell(f"""\
                            cd {params.reference_folder}
                            f"tar -hcf - {req} | pigz -5 -p {THREADS} >{req}.{EXT}" 
                        """)
            else:
                print('no references given')

            ## inputs ##
            def _remove_tar_ext(f: str):
                exts = '.tgz .tar.gz'.split(' ')
                for ext in exts:
                    if f.endswith(ext): f = f[:-len(ext)]
                return f

            zipped = dict((_remove_tar_ext(f), f) for f in os.listdir(zipped_inputs)) if zipped_inputs is not None else {}
            to_zip = [f for f in os.listdir('inputs') if f not in zipped]
            to_link = [zipped[f] for f in os.listdir('inputs') if f in zipped]

            if zipped_inputs is not None:
                for f in to_link:
                    os.symlink(zipped_inputs.joinpath(f), f'inputs/{f}')

            _shell(f"""\
                cd inputs
                {NEWL.join(f"tar -hcf - {f} | pigz -5 -p {THREADS} >{f}.{EXT}" for f in to_zip)}
            """)

            ## limes_x env ##
            import limes_x
            src = os.path.abspath(Path(os.path.dirname(inspect.getfile(limes_x))).joinpath('..'))
            _shell(f"""\
                cd {src}
                tar --exclude=__pycache__ -hcf - {limes_x.__name__} | pigz -5 -p {THREADS} >{HERE}/{self._SRC_FOLDER_NAME}.{EXT}
            """)
            if prerun is not None: prerun(inputs_dir)
        super().__init__(execute_procedure, _prepare_run)

    def Run(self, instance: JobInstance, workspace: Path, params: Params) -> JobResult:
        job = self._make_job(instance, workspace, params, _save=False)
        threads = params.logistic_threads if params.logistic_threads is not None else params.threads
        if instance.step.is_logistical:
            res = super().Run(instance, workspace, params)
            out_dir = job.context.output_folder
            BL = {"context.json", "result.json"}
            to_zip = []
            for f in os.listdir(out_dir):
                if f in BL: continue
                to_zip.append(f)
            NL = '\n'
            LiveShell(f"""\
                cd {job.context.output_folder}
                {NL.join(f"tar -hcf - {o} | pigz -5 -p {threads} >{o}.{self._EXT}" for o in to_zip)}
            """.replace("  ", ""), echo_cmd=False)
            return res
        job.context.Save(workspace)

        from ..environments import cloud
        entry_point = Path(os.path.abspath(inspect.getfile(cloud)))
        job.run_command  = f"""\
            python {entry_point} {job.instance.step.location} {workspace} {job.context.output_folder} \
                {workspace.joinpath(f'{self._SRC_FOLDER_NAME}.{self._EXT}')} SLURM_TMPDIR {":".join(self._NO_ZIP)} \
        """.replace("  ", "")
        success, msg = self._execute_procedure(job)
        return self._compile_result(job, success, msg)
