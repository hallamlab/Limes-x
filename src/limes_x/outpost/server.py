import pickle
from re import L
from quart import Quart, request
import os
from pathlib import Path
from typing import Any
import pickle, gzip, base64

from .config import VER
from .models import Context, Message
from ..models import Transform
from ..compute_module import ComputeModule

# Sockets may be more correct here
# but a transactional server is just easier

class Service:
    def __init__(self, home: Path|str) -> None:
        self.home = Path(home)
        self.modules: list[ComputeModule] = []
        self.compression_level = 3
    
    def _pack(self, data: Any):
        return base64.urlsafe_b64encode(
            gzip.compress(
                pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL),
                compresslevel=self.compression_level
            ),
        ).decode("ascii")
    
    def _unpack(self, raw: str):
        return pickle.loads(gzip.decompress(base64.urlsafe_b64decode(raw)))

    def set_home(self, payload: Any):
        if isinstance(payload, Path):
            home = payload
        # elif isinstance(payload, dict) and "home" in payload:
        #     home = Path(payload["home"])
        else:
            return
        if home.exists() and home.is_dir():
            self.home = home

    def reload_modules(self, payload: Any):
        cm_dir = self.home.joinpath("compute_modules")
        if not (cm_dir.exists() and cm_dir.is_dir()):
            return
        
        for dir in os.listdir(cm_dir):
            mpath = cm_dir.joinpath(dir)
            try:
                module = ComputeModule(mpath)
                self.modules.append(module)
            except AssertionError:
                continue

    def list_transforms(self, payload: Any):
        return [m.transform for m in self.modules]

    def Handle(self, msg: Message) -> Message:
        
        fn_name = msg.context.name.lower()
        if hasattr(self, fn_name):
            try:
                content = self._unpack(msg.payload)
                result = getattr(self, fn_name)(content)
                return Message(msg.key, Context.RESPONSE, self._pack(result))
            except Exception as e:
                return Message(msg.key, Context.ERROR, f"error [{e}]")

        return Message(msg.key, Context.ERROR, ["I don't know what to do with this", msg.context, msg.payload])

pid = None

api_app = Quart(__name__)
service = Service("./")

@api_app.route('/', methods=['GET'])
async def home():
    return f"Limes-x api! pid:{pid}"

@api_app.route(f'/{VER}/', methods=['POST'])
async def api():
    data = await request.get_json(silent=True)
    try:
        m = Message.FromDict(data)

        if m.context == Context.PING:
            res = Message(m.key, Context.PING, m.payload)
        else:
            res = service.Handle(m)
    except Exception as e:
        res = Message("", Context.ERROR, f"critical format error, [{e}]")
    
    print(res)
    if res.context == Context.ERROR:
        return res.ToDict(), 500
    else:
        return res.ToDict(), 200

@api_app.route(f'/{VER}/register_pid', methods=['POST'])
async def register_pid():
    data = await request.get_json()
    global pid
    pid = data.get("pid")
    return dict(echo=data)

@api_app.route(f'/{VER}/get_pid', methods=['GET'])
async def get_pid():
    return dict(pid=pid)

@api_app.route(f'/{VER}/kill', methods=['GET'])
async def kill():
    os.system(f"kill {pid}")
    return dict(notice="killed")
