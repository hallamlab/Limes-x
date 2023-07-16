#!/home/tony/Utilities/mambaforge/envs/lx/bin/python
# vals = [256**4-1, 12345, 2]

# def _places(v):
#     i = 0
#     while 256**i-1 < v:
#         i += 1
#     return i
# for x in vals:
#     p = _places(x)
#     xx = x.to_bytes(p)
#     print(p, [b for b in xx], int.from_bytes(xx))

import requests

# res = requests.post("http://localhost:12100/v1", json=dict(cmd="get_pid"))
res = requests.post("http://localhost:12100/v1", json=dict(cmd="kill"))

print(res.text)
