import sys, os
import json
import subprocess

MODULE_PATH = '/'.join(os.path.realpath(__file__).split('/')[:-2])
ENV = {}
with open(f'env.json') as env_file:
    ENV = json.load(env_file)

os.chdir(f"{ENV.get('workspace', './')}")
sys.path=ENV.get('PYTHONPATH', sys.path)

from abstract_pipeline.compute_modules import ComputeModule, Manifest
from abstract_pipeline.constants import DATA_FOLDER

ME = ComputeModule.LoadFromDisk(MODULE_PATH)
INPUTS = ME.GetInputManifests('./')
OUTPUT_TEMPLATES = ME.GetOutputTemplates()
OUTPUT_DIR = f'./{DATA_FOLDER}/{ME.name}'

def RegisterOutput(manifest: Manifest):
    return ME.RegisterOutput('./', manifest)

def Shell(cmd: str):
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
