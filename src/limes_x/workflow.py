from __future__ import annotations
import os, sys
from pathlib import Path
from typing import Any, Callable, Iterable
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
    def __init__(self, workspace: Path, steps: list[ComputeModule], **kwargs) -> None:
        super().__init__(_key=kwargs.get('_key'))
        self._ids: set[str] = set()
        self._job_instances: dict[str, JobInstance] = {}
        self._job_signatures: dict[str, JobInstance] = {} # key is "hash" of inputs
        self._item_lookup: dict[str, list[ItemInstance]] = {}
        self._given_item_instances: list[str] = []

        self._pending_jobs: dict[str, JobInstance] = {}
        self._item_instance_reservations: dict[ItemInstance, set[JobInstance]] = {}

        self._steps = steps
        self._finihsed_steps: set[str] = set()
        self._completed_modules: list[str] = []
        self._input_to_steps = self._make_input_to_steps_mapping(steps)
        self._group_by_paths: dict[tuple[str, str], list[str]] = {}
        for s in steps:
            for target, start in s._group_by.items():
                group_by_path = self._find_group_by_path(start.key, target.key)
                assert group_by_path is not None, f"[{target.key}] group by [{start.key}] for [{s.name}] is invalid for this set of compute modules. No path between"
                self._group_by_paths[(target.key, start.key)] = group_by_path

        self._changed = False
        self._workspace:Path = workspace

    def _make_input_to_steps_mapping(self, steps: list[ComputeModule]):
        mapping: dict[str, list[ComputeModule]] = {}
        for s in steps:
            for i in s.inputs:
                mapping[i.key] = mapping.get(i.key, []) + [s]
        return mapping

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
            if len(m._group_by)>0: md["input_groups"] = dict((k.key, v.key) for k, v in m._group_by.items())
            md["out"] = [i.key for i in m.outputs]
            if len(m.output_mask)>0: md["unused_out"] = [i.key for i in m.output_mask]
            modules[m.name] = md
        
        state = {
            "modules": modules,
            "module_executions": jobs_by_step,
            "completed_modules": self._completed_modules,
            "item_instances": item_instances,
            "given": self._given_item_instances,
            "item_instance_reservations": dict((ii.GetID(), [ji.GetID() for ji in jis]) for ii, jis in self._item_instance_reservations.items()),
            "pending_jobs": list(self._pending_jobs),
        }
        with open(self._workspace.joinpath(self._FILE_NAME), 'w') as j:
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
                    ji.complete = True
                    job_instances[id] = ji

                    outs = obj.get("outputs")
                    if outs is not None:
                        job_outputs[ji.GetID()] = outs

                if not found:
                    raise ValueError("failed to load state, the save may be corrupted")

            for jid, outs in job_outputs.items():
                outs = dict((ik, item_instances[v] if isinstance(v, str) else [item_instances[iik] for iik in v]) for ik, v in outs.items())
                job_instances[jid].MarkAsComplete(outs)

            state = WorkflowState(workspace, steps, _key=cls._initializer_key)
            state._completed_modules = serialized_state["completed_modules"]
            state._given_item_instances = list(given)
            state._ids.update(item_instances)
            state._ids.update(job_instances)
            state._job_signatures = dict((state._get_signature(list(ji.inputs.values())), ji) for ji in job_instances.values())
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
        workspace = Path(workspace)
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

    def _get_signature(self, inputs: list[ItemInstance|list[ItemInstance]]):
        inputs_list = [ii for g in [g if isinstance(g, list) else [g] for g in inputs] for ii in g]
        input_keys = sorted(ii.GetID() for ii in inputs_list)
        signature = "-".join(input_keys)
        return signature

    def GetPendingJobs(self):
        return list(self._pending_jobs.values())        

    def _find_group_by_path(self, start: str, target: str):
        class Todo:
            def __init__(self, node: str, path: list[str]) -> None:
                self.node = node
                self.path = path

        todo: list[Todo] = [Todo(start, [])]
        candidate = []
        while len(todo)>0:
            t = todo.pop()
            curr, path = t.node, t.path
            if curr == target and len(path)+1 > len(candidate):
                candidate = path+[curr] # found one, but want longest

            for job in self._input_to_steps.get(curr, []):
                todo += [Todo(o.key, path+[curr, job.name]) for o in job.GetUnmaskedOutputs()]
        return None if len(candidate) == 0 else candidate

    def _group_by(self, target: str, by: str):
        if by not in self._item_lookup: return {} # item to group by hasn't been made yet
        # instance may be used more than once by same compute module
        # due to cross/product of 2 or more inputs as lists
        starting_points = [ii for ii in self._item_lookup[by]] 
        if len(starting_points) == 0: return {}
        if target == by: return dict((i, [i]) for i in self._item_lookup[target])

        # can't just do tree search since some paths may not reach target
        path = self._group_by_paths.get((target, by))
        if path is None: return {} # not valid grouping, there is an assert in the constructor

        def _get_group(start: ItemInstance):
            class Todo:
                def __init__(self, node: ItemInstance|JobInstance, depth: int) -> None:
                    self.node = node
                    self.depth = depth

            group: list[ItemInstance] = []
            todo = [Todo(start, 0)]
            while len(todo)>0:
                t = todo.pop(0)
                instance, depth = t.node, t.depth
                if isinstance(instance, ItemInstance) and instance.item_name == target:
                    group.append(instance)
                    continue # found leaf (target) of @start

                next_name = path[depth+1]
                if isinstance(instance, ItemInstance):
                    res = [j for j in self._item_instance_reservations.get(instance, []) if j.step.name == next_name]
                    if len(res) == 0: return [] # item is intermediate and not used, so chain broken
                    for j in res:
                        todo.append(Todo(j, depth+1))
                else:
                    if not instance.complete: return [] # pending job found in group
                    outs = instance.ListOutputInstances()
                    if outs is None: continue # was marked complete, so maybe just a failed job
                    for i in outs:
                        if i.item_name != next_name: continue
                        todo.append(Todo(i, depth+1))
            return group

        groups: dict[ItemInstance, list[ItemInstance]] = {}
        for s in starting_points:
            g = _get_group(s)
            if len(g)==0: continue
            groups[s] = g
        return groups

    def Update(self):
        def _satisfies(module: ComputeModule):
            for i in module.inputs:
                if i.key not in self._item_lookup: return False
            return True

        class _namespace:
            def __init__(self) -> None:
                self._space: dict[str, list[ItemInstance]] = {} # key is item
                self._grouped_by: dict[str, ItemInstance] = {} # key is item

            def Copy(self):
                new = _namespace()
                new._space = self._space.copy()
                new._grouped_by = self._grouped_by.copy()
                return new

            def Add(self, instances: ItemInstance|list[ItemInstance]):
                if isinstance(instances, list):
                    item_name = next(iter(instances)).item_name
                    to_add = instances
                else:
                    item_name = instances.item_name
                    to_add = [instances]
                self._space[item_name] = self._space.get(item_name, []) + to_add

            # root is the "by" in group by
            def GetRootInstance(self, item_name: str):
                return self._grouped_by.get(item_name)

            def RegisterRootInstance(self, instance: ItemInstance):
                self._grouped_by[instance.item_name] = instance

        class Namespaces:
            def __init__(self) -> None:
                self.namespaces: list[_namespace] = [_namespace()]

            def Cross(self, group: list[ItemInstance]):
                new_nss: list[_namespace] = []
                for ns in self.namespaces:
                    for iis in group:
                        new = ns.Copy()
                        new.Add(iis)
                        new_nss.append(new)
                self.namespaces = new_nss

            def MergeGroup(self, group: dict[ItemInstance, list[ItemInstance]]):
                root_item_name = next(iter(group)).item_name
                roots = {r for r in [ns.GetRootInstance(root_item_name) for ns in self.namespaces] if r is not None}
                roots = roots.intersection(group)

                intersection = []
                for ns in self.namespaces:
                    for k in roots:
                        ns_root = ns.GetRootInstance(root_item_name)
                        if ns_root is None or ns_root.GetID() != k.GetID(): continue
                        ns.Add(group[k])
                        intersection.append(ns)
                self.namespaces = intersection

            def CrossGroup(self, group: dict[ItemInstance, list[ItemInstance]]):
                new_nss: list[_namespace] = []
                for ns in self.namespaces:
                    for root, iis in group.items():
                        new = ns.Copy()
                        new.Add(iis)
                        new.RegisterRootInstance(root)
                        new_nss.append(new)
                self.namespaces = new_nss

            def Compile(self):
                return [ns._space for ns in self.namespaces]

        def _gather_inputs(module: ComputeModule):
            input_groups: dict[str, list[ItemInstance]|dict[ItemInstance, list[ItemInstance]]] = {}
            for input in module.inputs:
                group_by = module.Grouped(input)
                if group_by is None:
                    instances = self._item_lookup.get(input.key)
                    if instances is None: return None
                    input_groups[input.key] = instances
                else:
                    group = self._group_by(input.key, group_by.key)
                    if len(group)==0: return None
                    input_groups[input.key] = group

            input_namespaces = Namespaces()
            seen_roots = set() # item names
            for item_name, dict_or_list in input_groups.items():
                if isinstance(dict_or_list, list):
                    input_namespaces.Cross(dict_or_list)
                else: # is dict
                    root = next(iter(dict_or_list)).item_name
                    if root in seen_roots:
                        input_namespaces.MergeGroup(dict_or_list)
                    else:
                        input_namespaces.CrossGroup(dict_or_list)
                        seen_roots.add(root)

            return input_namespaces.Compile()

        def _no_single_lists(ii: ItemInstance|list[ItemInstance]):
            if isinstance(ii, ItemInstance):
                return ii
            else:
                return ii[0] if len(ii)==1 else ii

        for module in self._steps:
            if not _satisfies(module): continue
            instances = _gather_inputs(module)
            if instances is None: continue
            for space in instances:
                signature = self._get_signature(list(space.values()))
                if signature in self._job_signatures: continue
                # print(module.name, space)

                job_inst = JobInstance(self._gen_id, module, dict((k, _no_single_lists(v)) for k, v in space.items()))
                self._job_signatures[signature] = job_inst
                self._pending_jobs[job_inst.GetID()] = job_inst
                self._job_instances[job_inst.GetID()] = job_inst
                for ii in job_inst.ListInputInstances():
                    lst = self._item_instance_reservations.get(ii, set())
                    lst.add(job_inst)
                    self._item_instance_reservations[ii] = lst
            self._changed = True

    def RegisterJobComplete(self, job_id: str, created: dict[Item, Any]):
        del self._pending_jobs[job_id]
        job_inst = self._job_instances[job_id]

        expected_outputs = job_inst.step.GetUnmaskedOutputs()
        outs: dict[str, ItemInstance|list[ItemInstance]] = {}
        for item, vals in created.items():
            if item not in expected_outputs: continue
            if not isinstance(vals, list): vals = [vals]
            insts = []
            for value in vals:
                inst = ItemInstance(self._gen_id, item, value, made_by=job_inst)
                self._register_item_inst(inst)
                insts.append(inst)
            outs[item.key] = insts if len(insts)>1 else insts[0]
        job_inst.MarkAsComplete(outs)

    def Invalidate(self, items: Iterable[Item]):
        old_save = self._workspace.joinpath(self._FILE_NAME)
        if not old_save.exists():
            print("invalidate did nothing since nothing has been done yet")
            return

        self._changed = True
        deleted_job_instances: set[JobInstance] = set()
        given_item_instances = set(self._given_item_instances)
        # invalidate parent and downstream
        def _invalidate(ii: ItemInstance):
            def _invalidate_one_item_instance(inst: ItemInstance):
                if inst.GetID() in given_item_instances: return
                if inst.item_name in self._item_lookup: del self._item_lookup[inst.item_name]
                if inst in self._item_instance_reservations: del self._item_instance_reservations[inst]

            parent = ii.made_by
            if parent is None:
                _invalidate_one_item_instance(ii)
                return
            
            todo = [parent]
            while len(todo)>0:
                curr = todo.pop()
                deleted_job_instances.add(curr)
                out = curr.ListOutputInstances()
                if out is None: continue
                for ii in out:
                    todo += [ji for ji in self._item_instance_reservations.get(ii, [])]
                    _invalidate_one_item_instance(ii)

        for item in items:
            for ii in list(self._item_lookup[item.key]):
                _invalidate(ii)

        # remove deleted jobs
        previous_iis: set[ItemInstance] = set()
        for ji in deleted_job_instances:
            previous_iis.update(ji.ListInputInstances())
            jk = ji.GetID()
            if jk in self._job_instances: del self._job_instances[jk]
            if jk in self._pending_jobs: del self._pending_jobs[jk]
            sig = self._get_signature(list(ji.inputs.values()))
            if sig in self._job_signatures: del self._job_signatures[sig]

        # remove item instance reservations of deleted jobs
        for ii in previous_iis:
            if ii not in self._item_instance_reservations: continue
            reservations = self._item_instance_reservations[ii]
            reservations = reservations.difference(deleted_job_instances)
            if len(reservations)>0:
                self._item_instance_reservations[ii] = reservations
            else:
                del self._item_instance_reservations[ii]

        i = 0
        previous_folder = Path()
        while True:
            i+=1
            previous_folder = self._workspace.joinpath(f'previous_run_{i:03}')
            if previous_folder.exists(): continue
            break
        
        deleted_jobs_folders = [ji.GetFolderName() for ji in deleted_job_instances]
        NL = '\n'
        cmd = f"""\
            mkdir -p {previous_folder}
            {NL.join(f"mv {self._workspace.joinpath(f)} {previous_folder.joinpath(f)}" for f in deleted_jobs_folders)}
            mv {old_save} {previous_folder}
        """
        os.system(cmd)

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
    OUTPUT_DIR = Path("outputs")
    def __init__(self, compute_modules: list[ComputeModule]|Path|str, reference_folder: Path|str) -> None:
        if isinstance(compute_modules, Path) or isinstance(compute_modules, str):
            compute_modules = ComputeModule.LoadSet(compute_modules)

        self._compute_modules = compute_modules
        self._reference_folder = Path(os.path.abspath(reference_folder))
        if not self._reference_folder.exists():
            os.makedirs(self._reference_folder)
        else:
            assert os.path.isdir(self._reference_folder), f"reference folder path exists, but is not a folder: {self._reference_folder}"
        self._solver = DependencySolver([c.GetTransform() for c in compute_modules])

    def Setup(self, install_type: str):
        for step in self._compute_modules:
            step.Setup(self._reference_folder, install_type)

    def _calculate(self, given: Iterable[Item], targets: Iterable[Item]):
        given_k = {x.key for x in given}
        targets_k = {x.key for x in targets} 
        steps, dep_map = self._solver.Solve(given_k, targets_k)
        return steps, dep_map

    def _check_feasible(self, steps: list[ComputeModule], targets: Iterable[Item], dep_map: dict[str, list[ComputeModule]]):
        targets = set(targets)
        products = set()
        for cm in steps:
            products = products.union(cm.outputs)
        missing = targets - products
        assert missing == set(), f"no module produces these items [{', '.join(str(i) for i in missing)}]"

        for cm in steps:
            deps = dep_map[cm.name]
            for i, g in cm._group_by.items():
                assert any(g in pre.inputs for pre in deps), f"invalid grouping: [{g.key}] is not upstream of [{i.key}] for module [{cm.name}]"

    def _link_output(self, paths: Path|list[Path]):
        if isinstance(paths, Path): paths = [paths]
        if not self.OUTPUT_DIR.exists(): os.makedirs(self.OUTPUT_DIR)
        for p in paths: # paths should be relative to ws
            toks = str(p).split('/')
            run_inst_dir = toks[0]
            fname = toks[-1]
            link = f"{run_inst_dir}--{fname}"
            os.symlink(f"../{p}", self.OUTPUT_DIR.joinpath(link))

    def Run(self, workspace: str|Path, targets: Iterable[Item],
        given: dict[Item, str|Path|list[str|Path]],
        executor: Executor, params: Params=Params(),
        regenerate: list[Item]=list(),
        _catch_errors: bool = True):
        if isinstance(workspace, str): workspace = Path(os.path.abspath(workspace))
        if not workspace.exists():
            os.makedirs(workspace)
        params.reference_folder = self._reference_folder

        # abs. path before change to working dir
        sys.path = [os.path.abspath(p) for p in sys.path]
        abs_path_if_path = lambda p: Path(os.path.abspath(p)) if isinstance(p, Path) else p
        abs_given = dict((k, [abs_path_if_path(p) for p in v] if isinstance(v, list) else [abs_path_if_path(v)]) for k, v in given.items())

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

        def _run():
            # make links for inputs in workspace
            input_dir = self.INPUT_DIR
            os.makedirs(input_dir, exist_ok=True)
            inputs: dict[Item, list[Path]] = {}
            for item, values in abs_given.items():
                parsed = []
                for p in values:
                    if isinstance(p, str):
                        parsed.append(p)
                        continue
                    assert os.path.exists(p), f"given [{p}] doesn't exist"
                    linked = input_dir.joinpath(p.name)
                    if linked.exists(): os.remove(linked)
                    os.symlink(p, linked)
                    parsed.append(linked)
                inputs[item] = parsed
            _steps, dep_map = self._calculate(inputs, targets)
            if _steps is False:
                print(f'no solution exists')
                return
            steps: list[ComputeModule] = [s.reference for s in _steps]
            print(f'linearized plan: [{" -> ".join(s.name for s in steps)}]')
            self._check_feasible(steps, targets, dep_map)
            state = WorkflowState.ResumeIfPossible('./', steps, inputs)
            if len(regenerate)>0:
                print(f'will regenerate [{", ".join([r.key for r in regenerate])}] and downstream dependents')
                state.Invalidate(regenerate)

            state.Update()
            state.Save()

            if len(state.GetPendingJobs()) == 0:
                print(f'nothing to do')
                return

            executor.PrepareRun(steps, self.INPUT_DIR, params)

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
                    print(f"{header} started")
                    _run_job_async(job, lambda: executor.Run(job, workspace, params.Copy(), targets))
                    jobs_ran[jid] = job

                try:
                    for result in sync.WaitAll():
                        if result is None:
                            raise KeyboardInterrupt()
                        job_instance = jobs_ran[result.made_by]
                        header = f"{_timestamp()} {job_instance.step.name}:{result.made_by}"
                        if not result.error_message is None:
                            print(f"{header} failed: [{result.error_message}]")
                        else:
                            print(f"{header} completed")
                        state.RegisterJobComplete(result.made_by, result.manifest)
                        if result.manifest is not None:
                            for t in targets:
                                if t in result.manifest:
                                    self._link_output(result.manifest[t])
                except KeyboardInterrupt:
                    print("force stopped")
                    return

                state.Update()
                state.Save()
            
            executor.PrepareRun

        original_dir = os.getcwd()
        def _wrap_and_run():
            os.makedirs(workspace, exist_ok=True)
            os.chdir(workspace)
            _run()
            print("done")

        if not _catch_errors:
            _wrap_and_run()
            os.chdir(original_dir)
        else:
            try:
                _wrap_and_run()
            except Exception as e:
                print(f"ERROR: {e}")
            finally:
                os.chdir(original_dir)
 