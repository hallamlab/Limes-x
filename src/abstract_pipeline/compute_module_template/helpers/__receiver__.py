import os
import json

HERE = os.path.realpath(__file__).split('/')[:-1]
_join_path = lambda p: '/'.join(p)
ENV = {}
with open(f'env.json') as env_file:
    ENV = json.load(env_file)

cmd = f'{_join_path(HERE)}/__launcher__.sh {_join_path(HERE[:-1])}/entry_point.py {":".join(ENV["PATH"])} {":".join(ENV["PYTHONPATH"])}'
os.system(cmd)
