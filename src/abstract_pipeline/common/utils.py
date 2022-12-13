import os
import uuid
from typing import IO, Callable
from inspect import signature
import subprocess
from threading import Condition, Thread
from queue import Queue

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

def LiveShell(cmd: str, onOut: Callable[[str], None]|None=None, onErr: Callable[[str], None]|None=None, echo_cmd: bool=True) -> int:
    class _Pipe:
        def __init__(self, io:IO[bytes]|None, lock: Condition=Condition(), q: Queue=Queue()) -> None:
            assert io is not None
            self.IO = io
            self.Lock = lock
            self.Q = q

    def callback(cb, msg):
        if cb is None:
            print(msg, end='')
        else:
            cb(msg)

    ENCODING = 'utf-8'

    # if isinstance(cmd, str):
    #     cmd = [cmd]
    # cmd = [c.strip() for c in cmd]

    process = subprocess.Popen(
        cmd,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # first = True
    # last_process = None
    # for c in cmd:
    #     p = subprocess.Popen(c, shell=True)
        
    #     first = False

    # callback(onOut, f'{" | ".join(cmd)}\n')
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
    if code is None: code = 1
    return code

###

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
