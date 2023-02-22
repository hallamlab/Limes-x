import os
import uuid
from typing import IO, Callable
from inspect import signature
import subprocess
from threading import Condition, Thread
from queue import Queue
import random
import sqlite3
from datetime import datetime as dt

def RemoveTrailingSlash(path: str):
    return path[:-1] if path[-1] == '/' else path

class PrivateInitException(Exception):
    def __init__(self) -> None:
        super().__init__(f'this class cant be initialized with a call, look for a classmethod')

class PrivateInit:
    _initializer_key: str = uuid.uuid4().hex

    def __init__(self, _key=None) -> None:
        if _key != self._initializer_key: raise PrivateInitException

class AutoPopulate:
    def __init__(self, **kwargs) -> None:
        for k, type_str in self.__annotations__.items():
            if k in kwargs:
                setattr(self, k, kwargs[k])
            else:
                setattr(self, k, None)

def CurrentTimeMillis():
    return round(time.time() * 1000)

def Timestamp():
    return f"{dt.now().strftime('%H:%M:%S')}>"

def LiveShell(cmd: str, onOut: Callable[[str], None]|None=None, onErr: Callable[[str], None]|None=None, echo_cmd: bool=True):
    class _Pipe:
        def __init__(self, io:IO[bytes]|None, lock: Condition=Condition(), q: Queue=Queue()) -> None:
            assert io is not None
            self.IO = io
            self.Lock = lock
            self.Q = q

    def callback(cb, msg):
        if cb is None:
            print(msg, end='\r')
        else:
            cb(msg)

    ENCODING = 'utf-8'
    process = subprocess.Popen(
        cmd,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if echo_cmd: callback(onOut, f'{cmd}\n')
    _in, _out, _err = [_Pipe(io) for io in [process.stdin, process.stdout, process.stderr]]
    _process = process

    workers: list[Thread] = []
    def reader(pipe: _Pipe, cb: Callable[[str], None]|None):
        io = iter(pipe.IO.readline, b'')
        while True:
            try:
                line = next(io)
            except (StopIteration, ValueError) as e:
                # print(f'err:{type(e)} {e.args}|')
                break
            chunk = bytes.decode(line, encoding=ENCODING)
            callback(cb, chunk)
    workers.append(Thread(target=reader, args=[_out, onOut]))
    workers.append(Thread(target=reader, args=[_err, onErr]))
        
    for w in workers:
        w.daemon = True # stop with program
        w.start()

    _process.wait()
    code = _process.poll()
    for w in workers:
        w.join()
    if code is None: code = 1
    return code

#######################################################################################################
# https://github.com/dmfrey/FileLock/blob/master/filelock/filelock.py
# with modification of using sqlite3 to prevent repeated deleting of lock file

import os
import time
import errno
 
class FileLockException(Exception):
    pass
 
class FileLock(object):
    """ A file locking mechanism that has context-manager support so 
        you can use it in a with statement. This should be relatively cross
        compatible as it doesn't rely on msvcrt or fcntl for the locking.
    """
 
    def __init__(self, file_name, timeout:float=60):
        """ Prepare the file locker. Specify the file to lock and optionally
            the maximum timeout and the delay between each attempt to lock.
        """
        self.lockfile = os.path.join(os.getcwd(), "%s.lock" % file_name)
        self.file_name = file_name
        self.timeout = timeout
        self.file_handle = None
 
 
    def acquire(self):
        """ Acquire the lock, if possible. If the lock is in use, it check again
            every `wait` seconds. It does this until it either gets the lock or
            exceeds `timeout` number of seconds, in which case it throws 
            an exception.
        """
        start_time = time.time()
        max_delay = self.timeout
        base_delay = 0.1
        delay_range = 0.5
        while True:
            try:
                # self.fd = os.open(self.lockfile, os.O_CREAT|os.O_DIRECT|os.O_RDWR)
                con = sqlite3.connect(self.lockfile, timeout=self.timeout)
                cur = con.cursor()
                cur.execute(f"create table if not exists x (id int primary key)")
                cur.execute(f"delete from x where true")
                self.file_handle = con
                break;
            except sqlite3.OperationalError as e:
                if self.timeout is None:
                    raise FileLockException("Could not acquire lock on {}".format(self.file_name))
                if (time.time() - start_time) >= self.timeout:
                    raise FileLockException("Timeout occured.")
                time.sleep((base_delay + delay_range*random.random()))
                # time.sleep(base_delay)
            delay_range = min(max_delay, 0.5 + 1.5*base_delay)
#        self.is_locked = True
 
 
    def release(self):
        """ Get rid of the lock by deleting the lockfile. 
            When working in a `with` statement, this gets automatically 
            called at the end.
        """
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None
 
 
    def __enter__(self):
        """ Activated when used in the with statement. 
            Should automatically acquire a lock to be used in the with block.
        """
        if self.file_handle is None:
            self.acquire()
        return self
 
 
    def __exit__(self, type, value, traceback):
        """ Activated at the end of the with statement.
            It automatically releases the lock if it isn't locked.
        """
        self.release()
 
 
    def __del__(self):
        """ Make sure that the FileLock instance doesn't leave a lockfile
            lying around.
        """
        self.release()

#######################################################################################################

class Overloader:
    """rudimentary, supposedly threadsafe, dispatch-style method overloading
    - register the class, then overload methods
    - disables type hints for overloaded functions
    - pylance indicates duplicate function names, use "# type: ignore" to ignore
    """

    def __init__(self) -> None:
        self._execute_later: list[Callable] = []

    def RegisterClass(self, cls):
        all_overloads: dict = {}
        for fn in self._execute_later:
            fn(all_overloads)

        def _make_dispatcher(fn_name, overloads):
            def _dispatcher(*args, **kwargs):
                for sig, candidate in overloads:
                    if len(args)+len(kwargs) > len(sig.parameters): continue
                    b = sig.bind_partial(*args, **kwargs)
                    return candidate(*b.args, **b.kwargs)
                raise TypeError(f"dispatcher for [{fn_name}] unable to find matching overload for [{args}] [{kwargs}]")
            return _dispatcher

        for fn_name, overloads in all_overloads.items():
            setattr(cls, fn_name, _make_dispatcher(fn_name, overloads))
        return cls

    def Overload(self, fn) -> Callable:
        def _later(all_overloads: dict):
            fn_name = fn.__name__
            fn_overloads = all_overloads.get(fn_name, [])
            fn_overloads.append((signature(fn), fn))
            # signature(fn).bind_partial().
            all_overloads[fn_name] = fn_overloads

        self._execute_later.append(_later)
        return fn

# # example

# o = Overloader()
# @o.WithOverloads
# class _X:
#     @o.Overload
#     def add(self, a: int, b): # type: ignore
#         print('int')
#         return a+b

#     @o.Overload
#     def add(self, a: float, b: float, c=False): # type: ignore
#         print('float')
#         return a+b

#     @o.Overload
#     def sub(self, a, b): # type: ignore
#         return a-b

#     @o.Overload
#     def sub(self, a, b, c): # type: ignore
#         return a-b-c

# x = _X()
# print(x.add(1, 2))
# print(x.sub(1, 2, 3))
