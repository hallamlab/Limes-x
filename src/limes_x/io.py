import subprocess
from typing import IO
from threading import Condition, Thread
from queue import Queue

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
