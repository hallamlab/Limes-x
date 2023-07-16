import os
import time
import asyncio
from pathlib import Path
from multiprocessing import Process

import asyncio
from typing import AsyncGenerator
from quart import Quart, websocket

import requests
from requests import ConnectionError

from .config import HOST, PORT, VER, GetConfig
from ..comms.schema import Pad, Unpad, BUFFER_SIZE

def StartServer():
    from .server import api_app
    from hypercorn.asyncio import serve

    asyncio.run(serve(api_app, GetConfig()))

def EnsureServer():
    host = f"http://{HOST}:{PORT}/{VER}"
    try:
        res = requests.get(f"{host}/get_pid")
        pid = res.json().get("pid")
    except ConnectionError:
        pid = os.fork()
        if pid == 0: # fork returns 0 in child and pid of child in parent
            StartServer()
            os._exit(0)
        
        for i in range(100):
            try:
                requests.post(f"{host}/register_pid", json=dict(pid=pid))
                break
            except ConnectionError:
                time.sleep(0.1)
                continue
    return pid
    
# class Connection:
#     def __init__(self) -> None:
#         try:
#             res = self.Send(dict())
#         except ConnectionError:
#             pid = StartServer()
#             self._register_pid(pid)
#             print(pid)

#     def Listen(self):
#         with open("/home/tony/workspace/python/Limes-all/Limes-x/test/outpost/cache/log", "w") as log:
#             log.write("x\n")
#             log.flush()
#             with open(0, "rb") as stdin:
#                 while True:
#                     x = stdin.read(BUFFER_SIZE)
#                     x = Unpad(x).decode()
                    
#                     log.write(x+"\n")
#                     log.write("xx\n")
#                     log.flush()

#     def _register_pid(self, pid: int):
#         for i in range(100):
#             try:
#                 self.Send(dict(cmd="register_pid", pid=pid))
#                 break
#             except ConnectionError:
#                 time.sleep(0.1)
#                 continue
            
#     def Send(self, data: dict):
#         res = requests.post(f"http://{HOST}:{PORT}/v1", json=data)
#         return res
        

    # def Start(self):
    #     def _child():
    #         from .server import api_app
    #         from .config import ui_config
    #         from hypercorn.asyncio import serve

    #         asyncio.run(serve(api_app, ui_config))

    #     def _parent():


    #     def _fork():
    #         if os.fork() != 0:
    #             _parent()
    #         else:
    #             _child()

    #     proc = Process(target=_fork)
    #     proc.start()
    #     proc.join()
    #     return