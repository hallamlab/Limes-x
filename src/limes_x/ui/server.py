import asyncio
from typing import AsyncGenerator
from quart import Quart, websocket

ui_app = Quart(__name__)

@ui_app.route('/')
async def hello():
    return 'limes ui!'

# @app.websocket('/ws')
# async def ws():
#     while True:
#         data = await websocket.receive()
#         await websocket.send(data)
