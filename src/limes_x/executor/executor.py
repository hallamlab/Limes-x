from dataclasses import dataclass, field
from enum import Enum, auto
from queue import Empty, Queue
from pathlib import Path
from threading import Condition, Thread

from ..compute_module import ComputeModule, ExecutionContext
from .models import Plan, Config
from ..utils import StdTime

def test(x):
    with open("/home/tony/workspace/python/Limes-all/Limes-x/test/mock_ws/cache/test_out.txt", "a") as f:
        f.write(f"{x}\n")

@dataclass
class State:
    active_jobs: list[Plan] = field(default_factory=lambda: list())

    def Save(self, folder: Path):
        fpath = folder.joinpath("executor_state")
        with open(fpath, "w") as f:
            f.write("todo: implement saving executor state")

@dataclass
class Agent:
    modules: dict[str, ComputeModule]
    plan: Plan

class Executor:
    def __init__(self, config: Config) -> None:
        self._lock = Condition()
        self._stopping = False
        self._worker = Thread(target=self._work_loop, args=[], daemon=True)
        self.q = Queue()

        self.state = State()
        self.config = config

    # -- async --

    def _follow_plan(self, agent: Agent):
        plan = agent.plan
        have = {plan.have[0].endpoint}
        for a in plan.execution_order:
            if a in plan.finished: continue
            if a in plan.in_progress: continue

            if all(e in have for e in a.used):
                mod = agent.modules[a.transform.key]
                context = ExecutionContext(
                    
                )
                mod.Execute(context)
    
    # see when jobs finish and update data models
    def _check_plan(self, plan: Plan):
        pass

    def _work_loop(self):
        s = self.state
        def process(plans: list[Plan]):
            for p in plans:
                self._follow_plan(p)
                s.active_jobs.append(p)

        def check():
            for p in s.active_jobs:
                self._check_plan(p)

        while True:
            new_plans: None|list[Plan] = None
            now = lambda: StdTime.CurrentTimeMillis()/1000
            last_update = now()
            INTERVAL = self.config.job_update_interval
            with self._lock:
                if self._stopping: break
                elapsed = now() - last_update
                delta = max(0, INTERVAL - elapsed)
                self._lock.wait(delta)

                if not self.q.empty():
                    try:
                        new_plans = self.q.get(timeout=0)
                    except ValueError as e:
                        pass

            if new_plans is not None:
                process(new_plans)

            if now() - last_update < INTERVAL: continue
            check()
            self.state.Save(self.config.home)
            last_update = now()

    # -- non async --

    def Start(self):
        self._worker.start()

    def Stop(self):
        with self._lock:
            self._stopping = True
            self._lock.notify_all()
        self._worker.join()

    def Register(self, plans: list[Plan]):
        plans = [p for p in plans if isinstance(p, Plan)]
        with self._lock:
            self.q.put(plans)
            self._lock.notify_all()
        return len(plans)
