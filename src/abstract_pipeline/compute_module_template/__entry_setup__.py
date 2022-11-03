import sys, os
import json

HERE = '/'.join(os.path.realpath(__file__).split('/')[:-1])
WORKSPACE = sys.argv[1]
ENV = {}
with open(f'{WORKSPACE}/env.json') as env_file:
    ENV = json.load(env_file)

sys.path = ENV.get('PYTHONPATH', "").split(':')

from abstract_pipeline.compute_modules import ComputeModule

ME = ComputeModule.LoadFromDisk(HERE)
