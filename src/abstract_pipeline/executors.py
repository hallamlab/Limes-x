from __future__ import annotations
import os, sys
from pathlib import Path
import time
from tkinter import E
from typing import Callable, Any, Iterable
import json
from .compute_module import ComputeModule, JobContext, JobResult
from .common.utils import AutoPopulate, LiveShell

class _with_hashable_id:
    __last_hash = 0
    def __init__(self, id: str) -> None:
        self.__id = id
        _with_hashable_id.__last_hash +=1
        self.__hash_val = _with_hashable_id.__last_hash

    def __hash__(self) -> int:
        return self.__hash_val

    def GetID(self):
        return self.__id

class JobInstance(_with_hashable_id):
    def __init__(self, id_gen: Callable[[int], str], step: ComputeModule, inputs: dict[str, ItemInstance|list[ItemInstance]]) -> None:
        super().__init__(id_gen(6))
        self.step = step

        self.inputs = inputs
        self._input_instances = self._flatten_values(self.inputs)

        self.outputs: dict[str, ItemInstance|list[ItemInstance]]|None = None
        self._output_instances: list[ItemInstance]|None = None

    def __repr__(self) -> str:
        return f"<ri: {self.step.name}>"

    def _flatten_values(self, data: dict[Any, ItemInstance|list[ItemInstance]]):
        insts: list[ItemInstance] = []
        for ii in data.values():
            if isinstance(ii, list):
                insts += ii
            else:
                insts.append(ii)
        return insts

    def ListInputInstances(self):
        return self._input_instances

    def AddOutputs(self, outs: dict[str, ItemInstance|list[ItemInstance]]):
        self.outputs = dict((i, v) for i, v in outs.items())
        self._output_instances = self._flatten_values(outs)

    def ListOutputInstances(self):
        return self._output_instances

    def ToDict(self):
        def _dictify(data: dict[str, ItemInstance|list[ItemInstance]]):
            return dict((k, v.GetID() if isinstance(v, ItemInstance) else [ii.GetID() for ii in v]) for k, v in data.items())

        self_dict = {
            "inputs": _dictify(self.inputs),
        }
        if self.outputs is not None:
            self_dict["outputs"] = _dictify(self.outputs)
        return self_dict

    @classmethod
    def FromDict(cls, step: ComputeModule, id: str, data: dict, item_instance_ref: dict[str, ItemInstance]):
        get_id = lambda _: id
        def _load(data: dict[str, str|list[str]]):
            loaded: dict[str, ItemInstance|list[ItemInstance]]= {}
            for k, v in data.items():
                if isinstance(v, str):
                    if v not in item_instance_ref: return None
                    iis = item_instance_ref[v]
                else:
                    if any(ii not in item_instance_ref for ii in v): return None
                    iis = [item_instance_ref[ii] for ii in v]
                candidate_items = [i for i in step.inputs if i.key==k]
                assert len(candidate_items) == 1
                item = candidate_items[0]
                loaded[item.key] = iis
            return loaded
                
        inputs = _load(data["inputs"])
        if inputs is None: return None

        inst = JobInstance(get_id, step, inputs)
        return inst

class ItemInstance(_with_hashable_id):
    def __init__(self, id_gen: Callable[[int], str], item_name:str, path: Path, made_by: JobInstance|None=None) -> None:
        super().__init__(id_gen(12))
        self.item_name = item_name
        self.path = path
        self.made_by = made_by
    
    def __repr__(self) -> str:
        return f"<ii: {self.item_name}>"

    def ToDict(self):
        self_dict = {
            "path": str(self.path),
        }
        if self.made_by is not None:
            self_dict["made_by"] = self.made_by.GetID()
        return self_dict
    
    @classmethod
    def FromDict(cls, item_name: str, id: str, data: dict, job_instance_ref: dict[str, JobInstance], given: set[str]):
        get_id = lambda _: id
        path = data["path"]
        made_by_id = data.get("made_by")

        if id not in given:
            if made_by_id not in job_instance_ref: return None
            made_by = job_instance_ref[made_by_id] if made_by_id is not None else None
        else:
            made_by = None # was given
        return ItemInstance(get_id, item_name, path, made_by=made_by)

class JobEnv(AutoPopulate):
    __FILE_NAME = 'env.json'
    shell_prefix: str
    context: JobContext

    def Save(self, ws: Path):
        with open(ws.joinpath(JobEnv.__FILE_NAME), 'w') as j:
            d = {}
            for k, v in self.__dict__.items():
                v: Any = v
                if v is None: continue
                v = { # switch
                    'context': lambda: v.ToDict(),
                }.get(k, lambda: str(v))()
                d[k] = v
            json.dump(d, j, indent=4)
            return d

    @classmethod
    def FromWorkspace(cls, ws: Path):
        with open(ws.joinpath(JobEnv.__FILE_NAME)) as j:
            d = json.load(j)
            kwargs = {}
            for k in d:
                v: Any = d[k]
                v = { # switch
                    'context': lambda: JobContext.FromDict(v),
                }.get(k, lambda: str(v))()
                kwargs[k] = v
            if 'shell_prefix' not in d:
                kwargs['shell_prefix'] = ''
            return JobEnv(**kwargs)

class Executor:
    def Run(self, job: JobInstance, context: JobContext) -> JobResult:
        raise NotImplementedError

    def _prep_filesystem(self, context: JobContext, env: JobEnv):
        out = context.output_folder
        os.makedirs(out, exist_ok=True)
        env.Save(out)
        return out

    def _get_result(self, context: JobContext, job: JobInstance) -> JobResult:
        result_json = context.output_folder.joinpath('result.json')
        if not os.path.exists(result_json):
            w = context.params.file_system_wait_sec
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
        if not err_msg is None: r.error_message = err_msg
        return r

    def _make_failed_result(self, job: JobInstance):
        r = JobResult()
        r.made_by = job.GetID()
        r.error_message = "executor failed"
        return r

class CondaExecutor(Executor):
    def __init__(self, env_name: str) -> None:
        self.env_name = env_name

    def Run(self, job: JobInstance, context: JobContext) -> JobResult:
        env = JobEnv()
        env.shell_prefix = f"conda run --no-capture-output -n {self.env_name}"
        env.context = context
        self._prep_filesystem(context, env)

        module_cmd = job.step.GenerateStaticRunCommand(Path(os.getcwd()), context.output_folder)

        code = LiveShell(module_cmd, echo_cmd=False)
        if code != 0:
            return self._make_failed_result(job)
        else:
            return self._get_result(context, job)
        
class TestExecutor(Executor):
    def __init__(self, additional_python_paths: Iterable[str|Path]) -> None:
        self._paths = [os.path.abspath(p) for p in additional_python_paths]

    def Run(self, job: JobInstance, context: JobContext) -> JobResult:
        env = JobEnv()
        env.context = context
        self._prep_filesystem(context, env)

        module_cmd = job.step.GenerateStaticRunCommand(Path(os.getcwd()), context.output_folder)
        code = LiveShell(f"export PYTHONPATH={':'.join(self._paths+sys.path)} && {module_cmd}", echo_cmd=False)
        if code != 0:
            return self._make_failed_result(job)
        else:
            return self._get_result(context, job)

