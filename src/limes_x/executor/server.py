import pickle
from re import L
from quart import Quart, request
import os
from pathlib import Path

from .models import Config, Context, Message
from .service import Service

# Sockets may be more correct here
# but a transactional server is just easier

service = Service(Config())
pid = os.getpid()
print(f"pid: {pid}")
api_app = Quart(__name__)

@api_app.route('/', methods=['GET'])
async def home():
    return f"Limes-x api! pid:{pid}"

# routes will not change if "ver" is changed in config...
@api_app.route(f'/{service.config.ver}/', methods=['POST'])
async def api():
    data = await request.get_data(as_text=True)
    try:
        m = Message.Unpack(data)

        if m.context == Context.PING:
            res = Message(Context.PING, m.payload, m.key, )
        else:
            res = service.Handle(m)
    except Exception as e:
        res = Message(Context.ERROR, f"critical format error, [{e}]", "")
    
    if res.context == Context.ERROR:
        return res.Pack(), 500
    else:
        return res.Pack(), 200

@api_app.route(f'/{service.config.ver}/get_pid', methods=['GET'])
async def get_pid():
    return dict(pid=pid)

@api_app.route(f'/{service.config.ver}/kill', methods=['GET'])
async def kill():
    os.system(f"kill {pid}")
    return dict(notice="killed")
