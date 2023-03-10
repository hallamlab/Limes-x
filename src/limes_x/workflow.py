from __future__ import annotations
import os, sys
import shutil
from pathlib import Path
from typing import Any, Callable, Iterable
import json
import uuid
from threading import Thread, Condition
import signal
from datetime import datetime as dt

from .execution.solver import DependencySolver
from .common.utils import PrivateInit, Timestamp
# from .compute_module import Item, ComputeModule, Params, JobContext, JobResult
from .execution.instances import JobInstance, ItemInstance
from .execution.modules import ComputeModule, Item, JobContext, JobResult, Params
from .execution.executors import Executor
from .execution.comms import FileSyncedDictionary

class JobError(Exception):
     def __init__(self, message=""):
        self.message = message
        super().__init__(self.message)

class WorkflowState(PrivateInit):
    _FILE_NAME = 'workflow_state.json'
    def __init__(self, workspace: Path, steps: list[ComputeModule], dependency_map: dict[str, set[str]]=dict(), **kwargs) -> None:
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
        self._parent_map: dict[str, set[str]] = dependency_map # item name to list[op_names]
        for s in steps:
            for i in s.inputs:
                self._add_dependency_mapping(i.key, s.name)
            for o in s.GetUnmaskedOutputs():
                self._add_dependency_mapping(s.name, o.key)

        self._group_by_paths: dict[tuple[str, str], list[str]] = {}
        for s in steps:
            for target, start in s._group_by.items():
                group_by_path = self._find_groupby_path(start.key, target.key)
                assert group_by_path is not None, f"[{target.key}] group by [{start.key}] for [{s.name}] is invalid for this set of compute modules. No path between"
                self._group_by_paths[(target.key, start.key)] = group_by_path

        self._changed = False
        self._workspace:Path = workspace

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
            md["in"] = [i.key for i in m.inputs]
            if len(m._group_by)>0:
                input_groups = {}
                for k, v in m._group_by.items():
                    input_groups[v.key] = input_groups.get(v.key, [])+[k.key]
                md["input_groups"] = input_groups
            md["out"] = [i.key for i in m.outputs]
            if len(m.output_mask)>0: md["unused_out"] = [i.key for i in m.output_mask]
            modules[m.name] = md
        
        state = {
            "modules": modules,
            "parent_map": dict((k, list(v)) for k, v in self._parent_map.items()),
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
                # group_by from module definition
                ins = {Item(i) for i in md["in"]}
                outs = {Item(i) for i in md["out"]}
                cm = cm_ref[name]
                assert cm.inputs == ins
                assert cm.outputs == outs
                cm.output_mask = {Item(i) for i in md.get("unused_out", [])}

            job_instances: dict[str, JobInstance] = {}
            item_instances: dict[str, ItemInstance] = {}

            todo_items = _flatten(serialized_state["item_instances"])
            todo_jobs = _flatten(serialized_state["module_executions"])
            job_outputs: dict[str, dict] = {}

            while len(todo_items)>0 or len(todo_jobs)>0:
                found = False
                len_todo = len(todo_items)
                for fi in range(len_todo):
                    i = len_todo-fi-1 # back to front
                    item_name, id, obj = todo_items[i]
                    ii = ItemInstance.FromDict(item_name, id, obj, item_instances, job_instances)
                    if ii is None: continue
                    found = True
                    todo_items.pop(i)
                    item_instances[id] = ii
                
                len_todo = len(todo_jobs)
                for fi in range(len_todo):
                    i = len_todo-fi-1 # back to front
                    module_name, id, obj = todo_jobs[i]
                    ji = JobInstance.FromDict(cm_ref[module_name], id, obj, item_instances)
                    if ji is None: continue
                    found = True
                    todo_jobs.pop(i)
                    job_instances[id] = ji

                    outs = obj.get("outputs")
                    if outs is not None:
                        job_outputs[id] = outs

                if not found:
                    raise ValueError("failed to load state, the save may be corrupted")

            for jid, outs in job_outputs.items():
                outs = dict((ik, item_instances[v] if isinstance(v, str) else [item_instances[iik] for iik in v]) for ik, v in outs.items())
                job_instances[jid].MarkAsComplete(outs)

            state = WorkflowState(workspace, steps,
                dependency_map=dict((k, set(v)) for k, v in serialized_state["parent_map"].items()),
                _key=cls._initializer_key)
            state._completed_modules = serialized_state["completed_modules"]
            state._given_item_instances = serialized_state["given"]
            state._ids.update(item_instances)
            state._ids.update(job_instances)
            state._job_instances = job_instances
            state._job_signatures = dict((state._get_signature(list(ji.inputs.values())), ji) for ji in job_instances.values())
            for k in serialized_state["pending_jobs"]:
                ji = job_instances[k]
                assert isinstance(ji, JobInstance)
                state._pending_jobs[k] = ji
            state._item_instance_reservations = dict(
                (item_instances[ik], {job_instances[rk] for rk in jids})
                for ik, jids in serialized_state["item_instance_reservations"].items()
            )

            for ii in item_instances.values():
                lst = state._item_lookup.get(ii.item_name, [])
                lst.append(ii)
                state._item_lookup[ii.item_name] = lst

            state.Update()
            return state

    @classmethod
    def MakeNew(cls, workspace: str|Path, steps: list[ComputeModule], given: list[InputGroup]):
        workspace = Path(workspace)
        assert len({m.name for m in steps})==len(steps), f"duplicate compute module name"
        dep_map = {}
        for grp in given:
            k = grp.root_type.key
            for ch in grp.children:
                to = dep_map.get(k, set())
                to.add(ch.key)
                dep_map[k] = to

        state = WorkflowState(workspace, steps, dependency_map=dep_map, _key=cls._initializer_key)
        for grp in given:
            root_instance = ItemInstance(state._gen_id, grp.root_type, grp.root_value)
            children: dict[str, ItemInstance|list[ItemInstance]] = {}
            for ii in [ItemInstance(state._gen_id, i, p) for i, ps in grp.children.items() for p in ps] + [root_instance]:
                state._register_item_inst(ii)
                state._given_item_instances.append(ii.GetID())
                v = children.get(ii.item_name, [])
                if not isinstance(v, list): v = [v]
                children[ii.item_name] =  v + [ii]
                if ii != root_instance: ii.made_by = root_instance

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
    def ResumeIfPossible(cls, workspace: str|Path, steps: list[ComputeModule], given: list[InputGroup]):
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

    def _get_signature(self, inputs: Iterable[ItemInstance|list[ItemInstance]]):
        inputs_list = [ii for g in [g if isinstance(g, list) else [g] for g in inputs] for ii in g]
        input_keys = sorted(ii.GetID() for ii in inputs_list)
        signature = "-".join(input_keys)
        return signature

    def GetPendingJobs(self):
        return list(self._pending_jobs.values())        

    def _add_dependency_mapping(self, start: str, end: str):
        mapped = self._parent_map.get(start, set())
        mapped.add(end)
        self._parent_map[start] = mapped

    def _find_groupby_path(self, start: str, target: str):
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

            for nnode in self._parent_map.get(curr, set()):
                if nnode in path: continue
                todo.append(Todo(nnode, path+[curr]))

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
                    if next_name in self._item_lookup:
                        for i in self._item_lookup[next_name]:
                            if i.made_by != instance: continue
                            todo.append(Todo(i, depth+1))
                        continue # item linked via logistical action, not by compute job
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
            input_groups: dict[str, dict[ItemInstance, list[ItemInstance]]] = {}
            for input in module.inputs:
                group_by = module.Grouped(input)
                if group_by is None:
                    instances = self._item_lookup.get(input.key)
                    if instances is None: return None
                    self_group = dict((i, [i]) for i in instances)
                    input_groups[input.key] = self_group
                else:
                    group = self._group_by(input.key, group_by.key)
                    if len(group)==0: return None
                    input_groups[input.key] = group

            input_namespaces = Namespaces()
            seen_roots = set() # item names
            # print(input_groups)
            for item_name, group in input_groups.items():
                root = next(iter(group)).item_name
                if root in seen_roots:
                    input_namespaces.MergeGroup(group)
                else:
                    input_namespaces.CrossGroup(group)
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
            # print(f"{module.name} {len(instances)}")
            for space in instances:
                signature = self._get_signature(list(space.values()))
                if signature in self._job_signatures: continue
                # print(module.name, space)

                job_inst = JobInstance(self._gen_id, module, dict((k, _no_single_lists(v)) for k, v in space.items()))
                self._register_job_instance(job_inst)
            self._changed = True

    def _register_job_instance(self, inst: JobInstance):
        self._job_signatures[self._get_signature(inst.inputs.values())] = inst
        self._pending_jobs[inst.GetID()] = inst
        self._job_instances[inst.GetID()] = inst
        for ii in inst.ListInputInstances():
            lst = self._item_instance_reservations.get(ii, set())
            lst.add(inst)
            self._item_instance_reservations[ii] = lst

    def RegisterJobComplete(self, job_id: str, created: dict[Item, Any]):
        if job_id not in self._pending_jobs: return
        job_inst = self._pending_jobs[job_id]
        del self._pending_jobs[job_id]

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
            print("invalidate did nothing since this is the first run")
            return
        self._changed = True

        # get steps and items to delete
        steps_to_delete: list[ComputeModule] = []
        for step in self._steps:
            if not any(o in items for o in step.outputs): continue
            steps_to_delete.append(step)
        todo = [s.name for s in steps_to_delete]
        names_to_delete = set()
        while len(todo)>0:
            curr = todo.pop()
            if curr in names_to_delete: continue
            names_to_delete.add(curr)
            todo += [c for c in self._parent_map.get(curr, [])]

        # get instances of each item and step
        given_item_instances = set(self._given_item_instances)
        item_instances_to_delete: list[ItemInstance] = []
        job_instances_to_delete: list[JobInstance] = []
        for k in names_to_delete:
            if k not in self._item_lookup: continue
            item_instances_to_delete += self._item_lookup[k]
        for ji in self._job_instances.values():
            if ji.step in steps_to_delete:
                job_instances_to_delete.append(ji) 

        # remove job instances
        for ji in job_instances_to_delete:
            jk = ji.GetID()
            if jk in self._job_instances: del self._job_instances[jk]
            if jk in self._pending_jobs: del self._pending_jobs[jk]
            sig = self._get_signature(list(ji.inputs.values()))
            if sig in self._job_signatures: del self._job_signatures[sig]

        # remove item instances
        for ii in item_instances_to_delete:
            if ii in given_item_instances: continue
            if ii.GetID() in given_item_instances: return
            if ii.item_name in self._item_lookup: del self._item_lookup[ii.item_name]
            if ii in self._item_instance_reservations:
                del self._item_instance_reservations[ii]
            else: 
                continue
            # remove item instance reservations of deleted jobs
            reservations = self._item_instance_reservations[ii]
            reservations = reservations.difference(job_instances_to_delete)
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
        
        deleted_jobs_folders = [ji.GetFolderName() for ji in job_instances_to_delete]
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

    try:
        import psutil

        current_process = psutil.Process()
        children = current_process.children(recursive=True)
        for child in children:
            child.kill()
    except ModuleNotFoundError:
        print("momdule <psutil> required to stop subprocesses, some may still be alive...")

class InputGroup:
    input_index = 0
    def __init__(self, by: tuple[Item, str|Path], children: dict[Item, str|Path|list[str]|list[Path]]) -> None:
        abs_path_if_path = lambda p: Path(os.path.abspath(p)) if isinstance(p, Path) else p
        root, root_value = by
        self.root_type = root
        self.root_value: str|Path = abs_path_if_path(root_value)
        self.children: dict[Item, list[str]|list[Path]] = dict((k, [abs_path_if_path(p) for p in v] if isinstance(v, list) else [abs_path_if_path(v)]) for k, v in children.items())

    def _record_input_paths(self, links: list[tuple[str, Path]], inputs_dir: Path):
        recorded_paths = set()
        paths_file = inputs_dir.joinpath("input_paths.tsv")
        if paths_file.exists():
            with open(paths_file) as tsv:
                for l in tsv:
                    toks = l.split('\t')
                    if len(toks) != 2: continue
                    recorded_paths.add(toks[0])
            
        new_paths = []
        for link, path in links:
            if link in recorded_paths: continue
            new_paths.append((link, str(path)))
        
        with open(paths_file, 'a') as tsv:
            TAB = '\t'
            tsv.writelines([f"{TAB.join(t)}\n" for t in new_paths])

    def LinkInputs(self, workspace: Path):
        here = os.getcwd()
        os.chdir(workspace)
        input_dir = Path(Workflow.INPUT_DIR)
        
        links = []
        def _fix(item, path):
            assert os.path.exists(path), f"given [{path}] doesn't exist"
            InputGroup.input_index+=1
            link_name = f"{InputGroup.input_index:04}--{path.name}"
            linked = input_dir.joinpath(link_name)
            os.symlink(path, linked)
            links.append((link_name, path))
            return linked

        if isinstance(self.root_value, Path):
            self.root_value = _fix(self.root_type, self.root_value)
        for item in list(self.children):
            parsed = []
            values = self.children[item]
            for p in values:
                if isinstance(p, str):
                    parsed.append(p)
                    continue
                linked = _fix(item, p)
                parsed.append(linked)
            self.children[item] = parsed
        self._record_input_paths(links, workspace)
        os.chdir(here)
    
    def ListItems(self):
        return [self.root_type] + list(self.children)

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

    def _link_output(self, job_instance: JobInstance, target: Item, values: str|Path|list[str]|list[Path]):
        _values: Any = values
        if not isinstance(values, list): _values = [values]
        if not self.OUTPUT_DIR.exists(): os.makedirs(self.OUTPUT_DIR)
        prefix = f"{job_instance.step.name}--{job_instance.GetID()}"
        for p in _values: # paths should be relative to ws
            if isinstance(p, Path) and p.exists():
                original = Path(f"../{p}")
                toks = str(p).split('/')
                fname = toks[-1]
                link = f"{prefix}.{fname}"
                os.symlink(original, self.OUTPUT_DIR.joinpath(link))
            else:
                with open(self.OUTPUT_DIR.joinpath(f"{prefix}.{target.key}.txt"), 'a') as out:
                    out.write(f"{p}\n")

    def Run(self, workspace: str|Path, targets: Iterable[Item],
        given: list[InputGroup],
        executor: Executor, params: Params=Params(),
        regenerate: list[Item]=list(),
        max_concurrent: int = 256,
        _catch_errors: bool = True,
    ):
        if isinstance(workspace, str): workspace = Path(os.path.abspath(workspace))
        if not workspace.exists():
            os.makedirs(workspace)
        params.reference_folder = self._reference_folder

        # abs. path before change to working dir
        sys.path = [os.path.abspath(p) for p in sys.path]

        result_sync = Sync()
        watcher = TerminationWatcher(result_sync)
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
                result_sync.PushNotify(result)
        
            th = Thread(target=_job, daemon=True)
            th.start()

        def _run():
            # make links for inputs in workspace
            inputs_dir = Workflow.INPUT_DIR
            if os.path.exists(inputs_dir): shutil.rmtree(inputs_dir)
            os.makedirs(inputs_dir)
            for g in given:
                g.LinkInputs(workspace=workspace)
            inputs = [i for g in [g.ListItems() for g in given] for i in g]
            _steps, dep_map = self._calculate(inputs, targets)
            if _steps is False:
                print(f'no solution exists')
                return
            steps: list[ComputeModule] = [s.reference for s in _steps]
            print(f'linearized plan: [{" -> ".join(s.name for s in steps)}]')
            self._check_feasible(steps, targets, dep_map)
            state = WorkflowState.ResumeIfPossible('./', steps, given)
            if len(regenerate)>0:
                print(f'will regenerate [{", ".join([r.key for r in regenerate])}] and downstream dependents')
                state.Invalidate(regenerate)

            state.Update()
            state.Save()

            with FileSyncedDictionary(workspace) as coms:
                coms.Clear()

            if len(state.GetPendingJobs()) == 0:
                print(f'nothing to do')
                return

            executor.PrepareRun(steps, self.INPUT_DIR, params)
            print(f">>> start")

            def sprint(x):
                with executor._sync:
                    print(x)

            jobs_ran = set() # this may be redundant
            jobs_running: dict[str, JobInstance] = {}
            while not watcher.kill_now:
                pending_jobs = state.GetPendingJobs()
                if len(pending_jobs) == 0: break

                for job in pending_jobs:
                    if watcher.kill_now:
                        raise KeyboardInterrupt()
                    if len(jobs_running) >= max_concurrent: break

                    jid = job.GetID()
                    if jid in jobs_ran: continue
                    sprint(f"{Timestamp()} queued {job.step.name}:{jid}")
                    _run_job_async(job, lambda: executor.Run(job, workspace, params.Copy()))
                    jobs_running[jid] = job
                    jobs_ran.add(jid)

                sys.stdout.flush()
                try:
                    for result in result_sync.WaitAll():
                        if result is None:
                            raise KeyboardInterrupt()
                        job_instance = jobs_running[result.made_by]
                        del jobs_running[result.made_by]
                        header = f"{job_instance.step.name}:{result.made_by}"
                        if not result.error_message is None:
                            sprint(f"{Timestamp()} failed {header}: [{result.error_message}]")
                            state.RegisterJobComplete(result.made_by, {})
                        else:
                            sprint(f"{Timestamp()} completed {header}")
                            state.RegisterJobComplete(result.made_by, result.manifest)
                        if result.manifest is not None:
                            for t in targets:
                                if t in result.manifest:
                                    self._link_output(job_instance, t, result.manifest[t])
                except KeyboardInterrupt:
                    print("force stopped")

                state.Update()
                state.Save()
                sys.stdout.flush()
            
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
                print(f"ERROR: in workflow {e}")
            finally:
                os.chdir(original_dir)
 