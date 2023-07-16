#!/home/tony/Utilities/mambaforge/envs/lx_net/bin/python

from __future__ import annotations
from genericpath import isdir
from pickle import TRUE
import os, sys
import subprocess
from typing import IO, Any, Callable
from threading import Condition, Thread
from queue import Queue, Empty
import json
import uuid
import traceback
import time
import asyncio
from asyncio import StreamReader, StreamWriter

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
                    print("!reader close")
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
        for p in [self._in, self._out, self._err]:
            with p:
                try:
                    if not p.IO.closed: p.IO.close()
                except (BrokenPipeError):
                    pass
                except RuntimeError as re:
                    if not re.args[0].startswith('reentrant call'): # todo, fix this
                        raise re

async def listen(shell: LiveShell):
    print(">>l")
    while not shell.IsClosed():
        errs, outs = shell.PollRead()
        for e in errs:
            print(e.decode(), end="")
        for o in outs:
            print(o.decode(), end="")
        await asyncio.sleep(1)
    print("<<l")

async def mock_input(shell: LiveShell):
    start = "ssh -tt 127.0.0.1 -i /home/tony/.ssh/local"
    # start = "ssh -tt sockeye"
    # for cmd in ["[[ $- == *i* ]] && stty -ixon", start]+"ls, pwd, touch testx, logout".split(", "):
    for cmd in ["[[ $- == *i* ]] && stty -ixon", start]+"ls, pwd, logout".split(", "):
        print(cmd)
        shell.Write(cmd)
        await asyncio.sleep(0.1 if cmd != "pwd" else 2)
    shell.Dispose()

async def count(n, x):
    for i in range(x):
        print(n, i)
        await asyncio.sleep(1)

async def main():
    with LiveShell() as shell:
        await asyncio.gather(
            listen(shell),
            mock_input(shell),
        )

    time.sleep(3)

asyncio.get_event_loop().run_until_complete(main())

