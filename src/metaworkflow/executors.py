from __future__ import annotations
import os, sys
from pathlib import Path
import time
from tkinter import E
from typing import Callable, Any, Iterable
import json
from .compute_module import ComputeModule, JobContext, JobResult, Params, Item
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
    __ID_LENGTH = 6
    def __init__(self, id_gen: Callable[[int], str], step: ComputeModule,
        inputs: dict[str, ItemInstance|list[ItemInstance]]) -> None:
        super().__init__(id_gen(JobInstance.__ID_LENGTH))
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

    def CreateContext(self, params: Params):
        c = JobContext()
        c.job_id = self.GetID()
        c.output_folder = Path(f"{self.step.name}--{self.GetID()}")
        c.params = params.Copy()
        c.manifest = dict((Item(k), [ii.path for ii in v] if isinstance(v, list) else v.path) for k, v in self.inputs.items())
        return c

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

class Job(AutoPopulate):
    instance: JobInstance
    context: JobContext
    run_command: str
    workspace: Path

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.context.Save(self.workspace)
        self.run_command = self.instance.step.GenerateStaticRunCommand(Path(os.getcwd()), self.context.output_folder)

    def Shell(self, cmd: str):
        err_log = []
        code=LiveShell(cmd, echo_cmd=False, onErr=lambda s: err_log.append(s))
        return code==0, "".join(err_log)

ExecutionHandler = Callable[[Job], tuple[bool, str]]
class Executor:
    execute_procedure: ExecutionHandler
    def __init__(self, execute_procedure: ExecutionHandler = lambda j: j.Shell(j.run_command)) -> None:
        self.execute_procedure = execute_procedure

    def Run(self, workspace: Path, instance: JobInstance, context: JobContext) -> JobResult:
        job = Job(
            instance = instance,
            context = context,
            workspace = workspace,
        )

        success, msg = self.execute_procedure(job)

        if not success:
            return self._make_failed_result(instance, msg)
        else:
            return self._get_result(context, instance)

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
        if not err_msg is None: r.error_message = err_msg
        return r

    def _make_failed_result(self, job: JobInstance, msg: str):
        r = JobResult()
        r.made_by = job.GetID()
        r.error_message = f"executor failed:\n{msg}"
        return r
