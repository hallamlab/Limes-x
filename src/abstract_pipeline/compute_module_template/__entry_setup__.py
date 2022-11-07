import sys, os
import json

MODULE_PATH = '/'.join(os.path.realpath(__file__).split('/')[:-1])
_WORKSPACE = sys.argv[1]
ENV = {}
with open(f'{_WORKSPACE}/env.json') as env_file:
    ENV = json.load(env_file)

sys.path = ENV.get('PYTHONPATH', [])

from abstract_pipeline.compute_modules import ComputeModule, Manifest
from abstract_pipeline.constants import DATA_FOLDER

ME = ComputeModule.LoadFromDisk(MODULE_PATH)
INPUTS = ME.GetInputManifests(_WORKSPACE)
OUTPUT_TEMPLATES = ME.GetOutputTemplates()
OUTPUT_DIR = f'{_WORKSPACE}/{DATA_FOLDER}'

def RegisterOutput(manifest: Manifest):
    return ME.RegisterOutput(_WORKSPACE, manifest)
