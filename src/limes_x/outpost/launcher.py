import os
import time
import asyncio
from pathlib import Path
import requests
from requests import ConnectionError

from .models import Config, Context, Message

def StartServer(config: Config):
    from .server import api_app
    from hypercorn.asyncio import serve

    if 0 == os.fork(): # fork returns 0 in child and pid of child in parent
        url = config.Url()
        for i in range(100):
            time.sleep(0.1)
            res = requests.post(url, data=Message(Context.SET_CONFIG, config).Pack())
            if res.status_code == 200:
                print(f"config [{config.home}] loaded")
                break
    else:
        asyncio.run(serve(api_app, config.HypercornConfig()))

def EnsureServer(config: Config):
    url = config.Url()
    try:
        res = requests.get(f"{url}/get_pid")
        pid = res.json().get("pid")
    except ConnectionError:
        pid = os.fork()
        if pid == 0: # fork returns 0 in child and pid of child in parent
            StartServer(config)
            os._exit(0)
    return pid
