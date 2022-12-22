from pathlib import Path
import psutil
import time
from typing import Any
from threading import Condition, Thread
from multiprocessing import Queue

class ResourceMonitor:
    _STOP = "stop"
    _c = 0

    def __init__(self, workspace: str|Path, delay_sec: int=60) -> None:
        if isinstance(workspace, str): workspace = Path(workspace)
        LOG_NAME = 'resources.log'

        def current_time_millis():
            return round(time.time() * 1000)
                
        def monitor(condition: Condition, inq: Queue, outq: Queue):
            log_msgs = []
            def _log(msg:str):
                # log_msgs.append([float(f) for f in msg.split('\t')])
                log_msgs.append(msg)
                
            # _log('###### monitor start [columns: runtime ms, cpu percent, GB available memory, GB used memory] ######')

            cpu_list: Any = lambda: psutil.cpu_percent(interval=None, percpu=True)
            # if not _running(): break
            start = current_time_millis()
            
            _stop = False
            def should_stop():
                nonlocal _stop
                if _stop: return True
                if inq.qsize()>0 and inq.get() == self._STOP:
                    _stop = True
                return _stop

            while True:
                mem = psutil.virtual_memory()
                now = current_time_millis()
                cpu_pct = cpu_list()
                _log(", ".join([
                    f'{(now-start)/1000:.1f}',
                    f'{sum(cpu_pct):5.1f}',
                    f'{mem.available/10**9:.1f}',
                    f'{mem.used/10**9:.1f}',
                ]))
                with condition:
                    if not should_stop(): condition.wait(delay_sec)
                    if should_stop():
                        outq.put(log_msgs)
                        break

        c = Condition()
        inq, outq = Queue(), Queue()
        worker = Thread(target=monitor, args=(c, inq, outq))
        worker.start()
        self._worker = worker
        self._condition = c
        self._inq = inq
        self._outq = outq
        self._stopped = False
        self._log = []

    def Stop(self) -> list[str]:
        if self._stopped: return self._log
        self._stopped = True
        with self._condition:
            self._inq.put(self._STOP)
            self._condition.notify_all()
        self._worker.join()

        with self._condition:
            log: list[str]|None = self._outq.get()
            if log is None:
                log = []
            self._log = log
            return self._log
