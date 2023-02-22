import os
import json
from pathlib import Path

from ..common.utils import FileLock

class FileSyncedDictionary:
    DEFAULT_COMMS = "comms"
    def __init__(self, workspace: Path, file_name: str|None=None, timeout: float=600) -> None:
        self._timeout = timeout
        if file_name is None: file_name = self.DEFAULT_COMMS
        self._lock_file = workspace.joinpath(file_name)
        self._data_file = workspace.joinpath(f"{file_name}.json")

        with FileLock(self._lock_file, timeout=self._timeout):
            if not os.path.exists(self._data_file):
                with open(self._data_file, 'w') as f:
                    json.dump({}, f)

        self._lock: FileLock|None = None
        self._data: dict = {}
        self._original_data: dict = {}

    def acquire(self):
        data = self._data
        if self._lock is None:
            self._lock = FileLock(self._lock_file, timeout=self._timeout)
            self._lock.acquire()
            if os.path.exists(self._data_file):
                with open(self._data_file) as j:
                    try:
                        data = json.load(j)
                    except json.JSONDecodeError:
                        data = {}
        self._data = data
        return CommsObject(data)
            
    def release(self):
        if self._lock is not None:
            l = self._lock
            self._lock = None
            with open(self._data_file, 'w') as j:
                json.dump(self._data, j, separators=(',', ':'))
            l.release()

    def __enter__(self):
        return self.acquire()
 
    def __exit__(self, type, value, traceback):
        return self.release()
 
    def __del__(self):
        self.release()

class CommsObject:
    IO_TASKS_KEY = 'io tasks'
    QUEUED_TASKS_KEY = 'queued io tasks'
    def __init__(self, data: dict) -> None:
        self._data = data

    def Clear(self):
        self._data.clear()

    def SwitchIoTaskToActive(self, key: str):
        q = self.GetIoTaskQueue()
        if key not in q: return
        q.remove(key)
        lst = self.GetIoTasks()
        if key not in lst:
            lst.append(key)

    def RemoveIoTask(self, key: str):
        q = self.GetIoTaskQueue()
        lst = self.GetIoTasks()
        if key in q: q.remove(key)
        if key in lst: lst.remove(key)

    def GetIoTasks(self) -> list[str]:
        if self.IO_TASKS_KEY not in self._data:
            self._data[self.IO_TASKS_KEY] = []
        return self._data[self.IO_TASKS_KEY]

    def GetIoTaskQueue(self) -> list[str]:
        if self.QUEUED_TASKS_KEY not in self._data:
            self._data[self.QUEUED_TASKS_KEY] = []
        return self._data[self.QUEUED_TASKS_KEY]

    def QueueIoTask(self, key: str):
        q = self.GetIoTaskQueue()
        if key not in q: q.append(key)

