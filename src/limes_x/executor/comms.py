import requests
from typing import Any

from sshtunnel import SSHTunnelForwarder, create_logger

from .models import Message, Context, Config as RemoteConfig
from ..models import Namespace, DataInstance, Endpoint, Transform, Solve, KeyGenerator

class Connection:
    def __init__(self, config: RemoteConfig) -> None:
        if config.host.lower() not in ["localhost", "127.0.01"]:
            self._tunnel = SSHTunnelForwarder(
                config.host,
                remote_bind_address=(config.host, config.port),
                logger=create_logger(),
            )
            self._url = f"http://localhost:{self._tunnel.ssh_port}/{config.ver}"
        else:
            self._tunnel = None
            self._url = f"http://{config.host}:{config.port}/{config.ver}"
        self._started = False

    def Url(self):
        return self._url

    def __enter__(self):
        if self._tunnel is None: return
        if self._started: return
        self._tunnel.start()
        self._started = True
        return self

    def __exit__(self):
        if self._tunnel is None: return
        if not self._started: return
        self._tunnel.stop()
        self._started = False
        return self

class Api:
    def __init__(self, host: str, connection: Connection) -> None:
        self._con = connection
        self.compression_level = 1
        
        code, _ = self.Ping()
        assert code == 200, f"failed to connect to outpost [{host}]"
    
    def _send(self, context: Context, payload: Any=None):
        res = requests.post(
            self._con.Url(),
            json=Message(
                context=context,
                payload=payload,
            ).Pack(),
        )

        msg = Message.Unpack(res.text)
        return res.status_code, msg

    def Ping(self, msg:str="hello"):
        return self._send(
            Context.PING,
            msg,
        )
    
    def ListTransforms(self) -> list[Transform]:
        code, msg = self._send(Context.LIST_TRANSFORMS)
        if code == 200:
            return [t for t in msg.payload if isinstance(t, Transform)]
        else:
            return []
