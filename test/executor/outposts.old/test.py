#!/home/tony/Utilities/mambaforge/envs/lx/bin/python

import os, sys
sys.path = list(set([
    "../../src",
]+sys.path))
import time
from typing import Any
import pickle, gzip
from limes_x.comms.connections import SshConnection
from limes_x.utils import KeyGenerator
from limes_x.comms.schema import BUFFER_SIZE, Transaction, Pad, Unpad


from sshtunnel import SSHTunnelForwarder
import requests

# with SshConnection("127.0.0.1", "/home/tony/.ssh/local") as ssh:
#     ssh.Write("cd /home/tony/workspace/python/Limes-all/Limes-x/src && /home/tony/Utilities/mambaforge/envs/lx/bin/python -m limes_x api")

server = SSHTunnelForwarder(
    'self',
    # ssh_username="pahaz",
    # ssh_password="secret",
    remote_bind_address=('127.0.0.1', 12100)
)

server.start()

print(server.local_bind_port)  # show assigned local port
# work with `SECRET SERVICE` through `server.local_bind_port`.



res = requests.post(
    f"http://localhost:{server.local_bind_port}/v1",
    json=dict(test="asdf")
)
print(res)
print(res.text)

server.stop()
# x="abcd"
# xx =x.encode()
# print(len(Pad(xx)))
# print(Unpad(Pad(xx)))
os._exit(0)
