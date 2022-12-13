from pathlib import Path
import psutil
import time
from typing import Any
import multiprocessing as mp 
from multiprocessing import Queue

class ResourceMonitor:
    _STOP = "stop"

    def __init__(self, workspace: str|Path, delay_sec: int=60) -> None:
        if isinstance(workspace, str): workspace = Path(workspace)
        LOG_NAME = 'resources.log'

        def current_time_millis():
            return round(time.time() * 1000)

        log_path = workspace.joinpath(LOG_NAME)
        def _log(msg):
            try:
                with open(log_path, 'a') as log:
                    log.write(f'{msg}\n')
            except FileNotFoundError:
                with open(log_path, 'w') as log:
                    log.write(f'{msg}\n')
                
        def monitor(condition, q: Queue):
            _log('###### monitor start [columns: runtime ms, cpu percent, GB available memory, GB used memory] ######')

            cpu_list: Any = lambda: psutil.cpu_percent(interval=None, percpu=True)
            # if not _running(): break
            start = current_time_millis()
            
            while True:
                mem = psutil.virtual_memory()
                now = current_time_millis()
                cpu_pct = cpu_list()
                _log("\t".join([
                    f'{(now-start)/1000:.1f}',
                    f'{sum(cpu_pct):5.1f}',
                    f'{mem.available/10**9:.1f}',
                    f'{mem.used/10**9:.1f}',
                ]))
                with condition:
                    condition.wait(delay_sec)
                if not q.empty():
                    msg = q.get()
                    if msg == self._STOP:
                        _log('###########################')
                        break

        c = mp.Condition()
        q = Queue()
        worker = mp.Process(target=monitor, args=(c, q))
        worker.start()
        self._worker = worker
        self._condition = c
        self._q = q

    def Stop(self):
        if not self._worker.is_alive(): return
        self._q.put_nowait(self._STOP)
        with self._condition:
            self._condition.notify()
        self._worker.join()
