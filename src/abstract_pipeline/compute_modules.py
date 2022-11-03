import os
import json

from .constants import DP_FOLDER, DATA_FOLDER
from .common.utils import _remove_trailing_slash

def GetDataPointerFolder(workspace: str):
    dp_dir = f'{workspace}/{DP_FOLDER}'
    return dp_dir
def GetDataFolder(workspace: str):
    dp_dir = f'{workspace}/{DATA_FOLDER}'
    return dp_dir

class ComputeModule:
    SAVE_FILE='module.json'
    ENTRY_POINT='entry_point.sh'
    def __init__(self, root: str, inputs: list[str], outputs: list[str]) -> None:
        self._root = os.path.abspath(_remove_trailing_slash(root))
        self.name = root.split('/')[-1]
        self._inputs = inputs
        self._outputs = outputs

    def _test_run(self, workspace: str):
        os.system(f'{self.GetEntryPoint()} {workspace}')

    def GetEntryPoint(self):
        return f'{self._root}/{self.ENTRY_POINT}'

    @classmethod
    def LoadFromDisk(cls, folder_path: str):
        save_path = f'{folder_path}/{cls.SAVE_FILE}'
        assert os.path.isfile(save_path), f"no [{cls.SAVE_FILE}] found at [{folder_path}]"
        assert os.path.isfile(f'{folder_path}/{cls.ENTRY_POINT}'), f"no [{cls.ENTRY_POINT}] found at [{folder_path}]"
        
        with open(save_path) as save:
            save_data = json.load(save)
            io = save_data['io']
            inputs = io['inputs']
            outputs = io['outputs']

            return ComputeModule(folder_path, inputs, outputs)

    def SaveToDisk(self):
        if not os.path.isdir(self._root):
            os.makedirs(self._root, exist_ok=True)

        ep = f'{self._root}/{self.ENTRY_POINT}'
        if not os.path.isfile(ep):
            with open(ep, 'w') as entry:
                entry.writelines([f'{line}\n' for line in [
                    '#!/bin/bash',
                    'PYTHONPATH=../../lib:${PYTHONPATH}', # todo: more robust >> others prolly need to be installed as pip packages
                    'SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )'
                    'echo $@'
                ]])
            os.chmod(ep, 0o775)

        sf = f'{self._root}/{self.SAVE_FILE}'
        if not os.path.isfile(sf):
            with open(sf, 'w') as save:
                save.write(json.dumps(dict(
                    io=dict(
                        inputs=self._inputs,
                        outputs=self._outputs
                    )
                ), indent=4))
