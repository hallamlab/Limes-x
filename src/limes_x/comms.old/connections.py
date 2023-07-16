from __future__ import annotations
import subprocess
from typing import IO, Any, Callable
from threading import Condition, Thread
from queue import Queue, Empty
import os
import time
import asyncio
import pickle
import gzip

from ..models import KeyGenerator
from .schema import Transaction, Pad, Unpad

# to future proof for other OS
class StaticShell:
    def __init__(self) -> None:
        self._buffer = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # this is sufficient for now (linux)
        os.system("\n".join(self._buffer))

    def Write(self, cmd: str):
        self._buffer.append(cmd)

class LiveShell:
    class Pipe:
        def __init__(self, io:IO[bytes]|None, lock: Condition=Condition(), q: Queue=Queue()) -> None:
            assert io is not None
            self.IO = io
            self.Lock = lock
            self.Q = q

        def __enter__(self):
            self.Lock.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.Lock.release()

    def __init__(self) -> None:
        console = subprocess.Popen(
            ["/bin/bash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self._console = console
        self._in = LiveShell.Pipe(console.stdin)
        self._out = LiveShell.Pipe(console.stdout)
        self._err = LiveShell.Pipe(console.stderr)
        self._onCloseLock = Condition()
        self._closed = False

        workers: list[Thread] = []
        def reader(pipe: LiveShell.Pipe):
            io = iter(pipe.IO.readline, b'')
            while True:
                if self.IsClosed():
                    return
                try:
                    line = next(io, None)
                    if line is None: continue
                except ValueError:
                    # print("val err")
                    break
                with pipe:
                    pipe.Q.put(line)
        workers.append(Thread(target=reader, args=[self._out]))
        workers.append(Thread(target=reader, args=[self._err]))

        for w in workers:
            w.daemon = True # stop with program
            w.start()

    def Send(self, payload: bytes):
        stdin = self._in
        with self._in:
            stdin.IO.write(payload)
            stdin.IO.flush()

    def Write(self, msg: str):
        self.Send(bytes('%s\n' % (msg), encoding="utf-8"))

    def PollRead(self):
        def _r(p: LiveShell.Pipe):
            with p:
                q = p.Q
                while not q.empty():
                    yield q.get_nowait()

        errs = list(_r(self._err))
        outs = list(_r(self._out))
        return errs, outs

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()
        return

    def IsClosed(self):
        with self._onCloseLock:
            return self._closed

    def Dispose(self):
        with self._onCloseLock:
            if self._closed:
                return
            self._closed = True
            self._onCloseLock.notify_all()

        self._console.terminate()
        # for p in [self._in, self._out, self._err]:
        #     with p:
        #         print(p.IO)
        #         try:
        #             if not p.IO.closed:
        #                 p.IO.flush()
        #                 p.IO.close()
        #         except BrokenPipeError:
        #             pass
        #         except RuntimeError as re:
        #             if not re.args[0].startswith('reentrant call'): # todo, fix this
        #                 raise re
        #         print("x", p.IO)

class SshConnection:
    def __init__(self, address: str, credentials: str, compression: int=3) -> None:
        assert compression > 0, "compression must be 1-9"
        self._shell = LiveShell()
        self._shell.Write(f"ssh -tt {address} -i {credentials}")
        self.compression=compression
        self.keygen = KeyGenerator()

    def Write(self, cmd: str):
        self._shell.Write(cmd)

    def Send(self, payload: bytes):
        self._shell.Send(Pad(payload))

    # def Transact(self, data: Any, timeout: int=1000, period: float=0.01):
    #     sh = self._shell
        
    #     key = self.keygen.GenerateUID()
    #     transaction = Transaction(data, key)
    #     package = gzip.compress(
    #         pickle.dumps(transaction, protocol=pickle.HIGHEST_PROTOCOL),
    #         compresslevel=self.compression
    #     )
        
    #     start = StdTime.CurrentTimeMillis()
    #     while True:
    #         now = StdTime.CurrentTimeMillis()
    #         if now-start>timeout: return

    #         sh.Send(package)
    #         time.sleep(period)
    #         errs, outs = sh.PollRead()
    #         for l in errs:
    #             print(f">>>", l.decode("utf-8"), end="")
    #         for l in outs:
    #             print(f"ERR", l.decode("utf-8"), end="")

    #         for l in outs:
    #             try:
    #                 response: Transaction = pickle.loads(gzip.decompress(l))
    #                 if response.key == transaction.key:
    #                     return response.payload
    #             except:
    #                 pass

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._shell.Write("logout")
        self._shell.Dispose()
