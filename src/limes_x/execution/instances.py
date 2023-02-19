from __future__ import annotations
import json
from sys import setprofile
from typing import Callable, Any
from pathlib import Path

from.modules import ComputeModule, Item

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
        self.complete = False

    def __repr__(self) -> str:
        return f"<ji: {self.step.name}>"

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

    def Invalidate(self):
        self.outputs = None
        self._output_instances = None
        self.complete = False

    def MarkAsComplete(self, outs: dict[str, ItemInstance|list[ItemInstance]]):
        self.outputs = dict((i, v) for i, v in outs.items())
        self._output_instances = self._flatten_values(outs)
        self.complete = True

    def ListOutputInstances(self):
        return self._output_instances

    def GetFolderName(self):
        return f"{self.step.name}--{self.GetID()}"

    def ToDict(self):
        def _dictify(data: dict[str, ItemInstance|list[ItemInstance]]):
            return dict((k, v.GetID() if isinstance(v, ItemInstance) else [ii.GetID() for ii in v]) for k, v in data.items())

        self_dict = {
            "complete": self.complete,
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
        outputs = _load(data["outputs"])
        if outputs is not None: inst.outputs = outputs
        inst.complete = data["complete"]
        return inst

class ItemInstance(_with_hashable_id):
    def __init__(self, id_gen: Callable[[int], str], item:Item, value: str|Path, made_by: JobInstance|None=None) -> None:
        super().__init__(id_gen(12))
        self.item_name = item.key
        self.value = value
        self.type = type(value)
        self.made_by = made_by
    
    def __repr__(self) -> str:
        return f"<ii: {self.item_name}:{self.GetID()}>"

    def ToDict(self):
        self_dict: dict[str, Any] = {
            "value": str(self.value),
            "type": str(self.type)
        }
        if self.made_by is not None:
            self_dict["made_by"] = self.made_by.GetID()
        return self_dict
    
    @classmethod
    def FromDict(cls, item_key: str, id: str, data: dict, job_instance_ref: dict[str, JobInstance], given: set[str]):
        get_id = lambda _: id
        type_str = data["type"]
        path_type_str = str(type(Path('')))
        value = data["value"]
        if type_str == path_type_str: value = Path(value)
        made_by_id = data.get("made_by")



        if id not in given:
            if made_by_id not in job_instance_ref: return None
            made_by = job_instance_ref[made_by_id] if made_by_id is not None else None
        else:
            made_by = None # was given
        return ItemInstance(get_id, Item(item_key), value, made_by=made_by)
