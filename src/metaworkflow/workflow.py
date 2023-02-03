from __future__ import annotations
import os, sys
from pathlib import Path
from typing import Callable, Iterable
import json
import uuid
from threading import Thread, Condition
import signal
from datetime import datetime as dt

from .execution.solver import DependencySolver
from .common.utils import PrivateInit
# from .compute_module import Item, ComputeModule, Params, JobContext, JobResult
from .execution.instances import JobInstance, ItemInstance
from .execution.modules import ComputeModule, Item, JobContext, JobResult, Params
from .execution.executors import Executor

class JobError(Exception):
     def __init__(self, message=""):
        self.message = message
        super().__init__(self.message)

class WorkflowState(PrivateInit):
    _FILE_NAME = 'workflow_state.json'
    def __init__(self, path: str|Path, steps: list[ComputeModule], **kwargs) -> None:
        super().__init__(_key=kwargs.get('_key'))
        self._ids: set[str] = set()
        self._job_instances: dict[str, JobInstance] = {}
        self._item_lookup: dict[str, list[ItemInstance]] = {}
        self._given_item_instances: list[str] = []

        self._pending_jobs: dict[str, JobInstance] = {}
        self._item_instance_reservations: dict[ItemInstance, set[JobInstance]] = {}

        self._steps = steps
        self._path = Path(path)
        self._changed = False

    def _register_item_inst(self, ii: ItemInstance):
        ilst = self._item_lookup.get(ii.item_name, [])
        ilst.append(ii)
        self._item_lookup[ii.item_name] = ilst

    def Save(self):
        if not self._changed: return
        jobs_by_step = {}
        for ji in self._job_instances.values():
            k = ji.step.name
            d = jobs_by_step.get(k, {})
            d[ji.GetID()] = ji.ToDict()
            jobs_by_step[k] = d

        item_instances = {}
        for k, instances in self._item_lookup.items():
            d = item_instances.get(k, {})
            for ii in instances: d[ii.GetID()] = ii.ToDict()
            item_instances[k] = d

        modules = {}
        for m in self._steps:
            md = {}
            ins = []
            for i in m.inputs:
                d = {"item": i.key}
                ins.append(d)
            md["in"] = ins
            md["input_groups"] = dict((k.key, v.key) for k, v in m._group_by.items())
            md["out"] = [i.key for i in m.outputs]
            if len(m.output_mask)>0: md["unused_out"] = [i.key for i in m.output_mask]
            modules[m.name] = md
        
        state = {
            "modules": modules,
            "module_executions": jobs_by_step,
            "item_instances": item_instances,
            "given": self._given_item_instances,
            "item_instance_reservations": dict((ii.GetID(), [ji.GetID() for ji in jis]) for ii, jis in self._item_instance_reservations.items()),
            "pending_jobs": list(self._pending_jobs),
        }
        with open(self._path.joinpath(self._FILE_NAME), 'w') as j:
            json.dump(state, j, indent=4)

    @classmethod
    def LoadFromDisk(cls, workspace: str|Path, steps: list[ComputeModule]):
        cm_ref = dict((c.name, c) for c in steps)
        workspace = Path(workspace)

        def _flatten(instances_by_type: dict):
            return [tup for g in [[(type, hash, data) for hash, data in insts.items()] for type, insts in instances_by_type.items()] for tup in g]

        with open(workspace.joinpath(cls._FILE_NAME)) as j:
            serialized_state = json.load(j)

            for name, md in serialized_state["modules"].items():
                ins = {Item(i["item"]) for i in md["in"]}
                outs = {Item(i) for i in md["out"]}
                cm = cm_ref[name]
                assert cm.inputs == ins
                assert cm.outputs == outs
                cm.output_mask = {Item(i) for i in md.get("unused_out", [])}

            job_instances: dict[str, JobInstance] = {}
            item_instances: dict[str, ItemInstance] = {}

            given = set(serialized_state["given"])
            todo_items = _flatten(serialized_state["item_instances"])
            todo_jobs = _flatten(serialized_state["module_executions"])
            job_outputs: dict[str, dict] = {}

            while len(todo_items)>0 or len(todo_jobs)>0:
                found = False
                while len(todo_items)>0:
                    item_name, id, obj = todo_items[0]
                    ii = ItemInstance.FromDict(item_name, id, obj, job_instances, given)
                    if ii is None: break
                    found = True
                    todo_items.pop(0)
                    item_instances[id] = ii
                
                while len(todo_jobs)>0:
                    module_name, id, obj = todo_jobs[0]
                    ji = JobInstance.FromDict(cm_ref[module_name], id, obj, item_instances)
                    if ji is None: break
                    found = True
                    todo_jobs.pop(0)
                    job_instances[id] = ji

                    outs = obj.get("outputs")
                    if outs is not None:
                        job_outputs[ji.GetID()] = outs

                if not found:
                    raise ValueError("failed to load state, the save may be corrupted")

            for jid, outs in job_outputs.items():
                outs = dict((ik, item_instances[v] if isinstance(v, str) else [item_instances[iik] for iik in v]) for ik, v in outs.items())
                job_instances[jid].AddOutputs(outs)

            state = WorkflowState(workspace, steps, _key=cls._initializer_key)
            state._given_item_instances = list(given)
            state._ids.update(item_instances)
            state._ids.update(job_instances)
            state._job_instances = job_instances
            state._pending_jobs = dict((k, job_instances[k]) for k in serialized_state["pending_jobs"])
            state._item_instance_reservations = dict(
                (item_instances[ik], {job_instances[rk] for rk in jids})
                for ik, jids in serialized_state["item_instance_reservations"].items()
            )

            for ii in item_instances.values():
                lst = state._item_lookup.get(ii.item_name, [])
                lst.append(ii)
                state._item_lookup[ii.item_name] = lst
            return state

    @classmethod
    def MakeNew(cls, workspace: str|Path, steps: list[ComputeModule], given: dict[Item, list[Path]]):
        assert len({m.name for m in steps})==len(steps), f"duplicate compute module name"
        state = WorkflowState(workspace, steps, _key=cls._initializer_key)
        for ii in [ItemInstance(state._gen_id, i, p) for i, ps in given.items() for p in ps]:
            state._register_item_inst(ii)
            state._given_item_instances.append(ii.GetID())

        produced: dict[Item, ComputeModule] = {}
        for step in steps:
            step.output_mask = set()
            for item in step.outputs:
                if item in produced:
                    print(f"[{item.key}] is already produced by [{produced[item].name}], masking this output of [{step.name}]")
                    step.MaskOutput(item)
                elif item in given:
                    print(f"[{item.key}] is given, masking this output of [{step.name}]")
                    step.MaskOutput(item)
                else:
                    produced[item] = step

        state.Update()
        return state

    @classmethod
    def ResumeIfPossible(cls, workspace: str|Path, steps: list[ComputeModule], given: dict[Item, list[Path]]):
        workspace = Path(workspace)
        if os.path.exists(workspace.joinpath(cls._FILE_NAME)):
            return WorkflowState.LoadFromDisk(workspace, steps)
        else:
            assert given is not None
            return WorkflowState.MakeNew(workspace, steps, given)

    def _gen_id(self, id_len: int):
        while True:
            id = uuid.uuid4().hex[:id_len]
            if id not in self._ids: break 
        self._ids.add(id)
        return id

    def _satisfies(self, module: ComputeModule):
        for i in module.inputs:
            if i.key not in self._item_lookup: return False
        return True

    def GetPendingJobs(self):
        return list(self._pending_jobs.values())        

    def _group_by(self, item_name: str, by_name: str):
        _parent_cache = {}
        groups: dict[ItemInstance, set[ItemInstance]] = {}
        if item_name == by_name: return dict((i, {i}) for i in self._item_lookup[item_name])

        one_path: list[ItemInstance|JobInstance] = []
        def _handle_base_case(candidate: ItemInstance, member: ItemInstance, path: list[ItemInstance|JobInstance]):
            if candidate in _parent_cache:
                parent = _parent_cache[candidate]
            elif candidate.item_name == by_name:
                nonlocal one_path
                if len(one_path)==0: one_path = path
                parent = candidate
            else:
                return False

            grp = groups.get(parent, set())
            grp.add(member)
            groups[parent] = grp
            for inst in path:
                _parent_cache[inst] = parent
            return True

        todo: list[tuple[ItemInstance, ItemInstance, list[ItemInstance|JobInstance]]] = [
            (ii, ii, [ii]) for ii in self._item_lookup[item_name]
        ]
        while len(todo)>0:
            original, ii, path = todo.pop() # dfs to maximize utility of parent cache
            if _handle_base_case(ii, original, path): continue

            ji = ii.made_by
            if ji is None: continue # is original input
            consumed = ji.ListInputInstances()
            todo += [(original, in_i, path+[ji, in_i]) for in_i in consumed]

        ## filter out incomplete groups ##

        # this should never fail since 
        # a path must have been traversed to find the parnet
        assert len(one_path)>0
        one_path_items = {ii.item_name for ii in one_path if isinstance(ii, ItemInstance)}

        def _group_complete(grouped_by: ItemInstance, group: set[ItemInstance]):
            while True:
                # have = tocheck
                jis = {ii.made_by for ii in group if ii.made_by is not None}
                if len(jis) == 0: continue # all original input
                produced = {ii for g in [ji.ListOutputInstances() for ji in jis] if g is not None for ii in g}

                if produced != group:
                    # produced more than have -> some not consumed
                    # this max size group is incomplete -> no groups were complete
                    return False

                consumed = {ii for g in [ji.ListInputInstances() for ji in jis] if g[0].item_name in one_path_items for ii in g}
                consumed_item = {ii.item_name for ii in consumed}

                if grouped_by.item_name in consumed_item: return True # reached parent, everything checks out
                group = consumed

        for k in list(groups):
            if not _group_complete(k, groups[k]):
                del groups[k]
        return groups

    def Update(self):
        for module in self._steps:
            if not self._satisfies(module): continue

            class Namespaces:
                def __init__(self) -> None:
                    self.namespaces: list[dict[str, ItemInstance|list[ItemInstance]]] = [{}]
                def AddMapping(self, i: str, inst_or_list: ItemInstance|list[ItemInstance]):
                    for ns in self.namespaces:
                        ns[i] = inst_or_list

                # assumes all instances are of the same item
                def Split(self, groups: list[list[ItemInstance]]):
                    def _kv(grp: list[ItemInstance]):
                        rep = grp[0].item_name
                        return (rep, grp if len(grp)>1 else grp[0])

                    if len(groups) == 1:
                        grp = groups[0]
                        self.AddMapping(*_kv(grp))

                    new_nss = []
                    for ns in self.namespaces:
                        for grp in groups:
                            clone = ns.copy()
                            rep, v = _kv(grp)
                            clone[rep] = v
                            new_nss.append(clone)
                    self.namespaces = new_nss

            namespaces = Namespaces()
            outer_continue = False
            for item in module.inputs:
                item_name = item.key
                instances: list[ItemInstance] = []
                for inst in self._item_lookup[item_name]:
                    jis = self._item_instance_reservations.get(inst, set())
                    if any(ji.step == module for ji in jis): continue
                    instances.append(inst)

                if len(instances) == 0:
                    outer_continue = True
                    break
                have_array = len(instances)>1
                item_grouped_by = module.Grouped(item)
                want_array = item_grouped_by is not None
                    
                ## join & group by ##
                if want_array:
                    groups = self._group_by(item_name, item_grouped_by.key)
                    for k in list(groups):
                        grp = groups[k]
                        rep = next(iter(grp))
                        if any(module in [ji.step for ji in self._item_instance_reservations.get(ii, set())] for ii in grp):
                            del groups[k]

                    namespaces.Split([list(g) for g in groups.values()])

                ## split, 1 each ##
                elif not want_array and have_array:
                    namespaces.Split([[i] for i in instances])

                ## 1 to 1 ##
                elif not want_array and not have_array:
                    namespaces.AddMapping(item_name, instances[0])

            if outer_continue: continue
            for ns in namespaces.namespaces:
                job_inst = JobInstance(self._gen_id, module, ns)
                self._pending_jobs[job_inst.GetID()] = job_inst
                self._job_instances[job_inst.GetID()] = job_inst
                for ii in job_inst.ListInputInstances():
                    lst = self._item_instance_reservations.get(ii, set())
                    lst.add(job_inst)
                    self._item_instance_reservations[ii] = lst
            self._changed = True

    def RegisterJobComplete(self, job_id: str, created: dict[Item, Path|list[Path]]):
        del self._pending_jobs[job_id]
        job_inst = self._job_instances[job_id]

        expected_outputs = job_inst.step.GetUnmaskedOutputs()
        outs: dict[str, ItemInstance|list[ItemInstance]] = {}
        for item, paths in created.items():
            if item not in expected_outputs: continue
            if not isinstance(paths, list): paths = [paths]
            insts = []
            for path in paths:
                inst = ItemInstance(self._gen_id, item, path, made_by=job_inst)
                self._register_item_inst(inst)
                insts.append(inst)
            outs[item.key] = insts if len(insts)>1 else insts[0]
        job_inst.AddOutputs(outs)

class Sync:
    def __init__(self) -> None:
        self.lock = Condition()
        self.queue = []

    def PushNotify(self, item: JobResult|None=None):
        with self.lock:
            self.queue.append(item)
            self.lock.notify()

    def WaitAll(self) -> list[JobResult|None]:
        with self.lock:
            if len(self.queue)==0:
                self.lock.wait()

            results = self.queue.copy()
            self.queue.clear()
            return results

class TerminationWatcher:
  kill_now = False
  def __init__(self, sync: Sync):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)
    self.sync = sync

  def exit_gracefully(self, *args):
    print('stop requested')
    self.kill_now = True
    self.sync.PushNotify()

class Workflow:
    INPUT_DIR = Path("inputs")
    def __init__(self, compute_modules: list[ComputeModule]|Path|str) -> None:
        if isinstance(compute_modules, Path) or isinstance(compute_modules, str):
            compute_modules = ComputeModule.LoadSet(compute_modules)

        self._compute_modules = compute_modules
        self._solver = DependencySolver([c.GetTransform() for c in compute_modules])

    def _calculate(self, given: Iterable[Item], targets: Iterable[Item]):
        given_k = {x.key for x in given}
        targets_k = {x.key for x in targets} 
        steps, dep_map = self._solver.Solve(given_k, targets_k)
        return steps, dep_map

    # this just does setup, _run actually executes the compute modules
    def Run(self, workspace: str|Path, targets: Iterable[Item],
        given: dict[Item, str]|dict[Item, Path]|dict[Item, list[str]]|dict[Item, list[Path]],
        executor: Executor, params: Params=Params()):
        if isinstance(workspace, str): workspace = Path(os.path.abspath(workspace))
        if not workspace.exists():
            os.makedirs(workspace)

        # abs. path before change to working dir
        sys.path = [os.path.abspath(p) for p in sys.path]
        abs_path = lambda p: Path(os.path.abspath(p))
        abs_given = dict((k, [abs_path(p) for p in v] if isinstance(v, list) else [abs_path(v)]) for k, v in given.items())

        def _timestamp():
            return f"{dt.now().strftime('%H:%M:%S')}>"

        sync = Sync()
        watcher = TerminationWatcher(sync)
        def _run_job_async(jobi: JobInstance, procedure: Callable[[], JobResult]):
            def _job():
                try:
                    result = procedure()
                except Exception as e:
                    result = JobResult(
                        exit_code = 1,
                        error_message = str(e),
                        made_by = jobi.GetID(),
                    )
                sync.PushNotify(result)
        
            th = Thread(target=_job)
            th.start()
            return th

        def _run_in_workspace():
            # make links for inputs in workspace
            input_dir = self.INPUT_DIR
            os.makedirs(input_dir, exist_ok=True)
            inputs: dict[Item, list[Path]] = {}
            for item, paths in abs_given.items():
                links = []
                for p in paths:
                    linked = input_dir.joinpath(p.name)
                    if linked.exists(): os.remove(linked)
                    os.symlink(p, linked)
                    links.append(linked)
                inputs[item] = links
            steps, _ = self._calculate(inputs, targets)
            if steps is False:
                print(f'no solution exists')
                return
            state = WorkflowState.ResumeIfPossible('./', [s.reference for s in steps], inputs)
            state.Save()

            if len(state.GetPendingJobs()) == 0:
                print(f'nothing to do')
                return

            jobs_ran: dict[str, JobInstance] = {}
            while not watcher.kill_now:
                pending_jobs = state.GetPendingJobs()
                if len(pending_jobs) == 0: break

                for job in pending_jobs:
                    if watcher.kill_now:
                        raise KeyboardInterrupt()

                    jid = job.GetID()
                    if jid in jobs_ran: continue
                    header = f"{_timestamp()} {job.step.name}:{jid}"
                    print(f"{header} started {'>'*(50-len(header))}")
                    _run_job_async(job, lambda: executor.Run(job, workspace, params))
                    jobs_ran[jid] = job

                try:
                    for result in sync.WaitAll():
                        if result is None:
                            raise KeyboardInterrupt()
                        job_instance = jobs_ran[result.made_by]
                        header = f"{_timestamp()} {job_instance.step.name}:{result.made_by}"
                        if not result.error_message is None:
                            print(f"{header} failed with msg: [{result.error_message}]")
                            return
                        else:
                            print(f"{header} completed")
                            state.RegisterJobComplete(result.made_by, result.manifest)
                except KeyboardInterrupt:
                    print("force stopped")
                    return
                state.Update()
                state.Save()

        original_dir = os.getcwd()
        try:
            os.makedirs(workspace, exist_ok=True)
            os.chdir(workspace)
            _run_in_workspace()
            print("done")
        finally:
            os.chdir(original_dir)
            return not watcher.kill_now
