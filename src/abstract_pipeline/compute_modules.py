import sys, os
import json
import uuid
import shutil
from .constants import DP_FOLDER, DATA_FOLDER
from .common.utils import _remove_trailing_slash
from data_pointer import DType, DataPointer

def GetDataPointerFolder(workspace: str):
    dp_dir = f'{workspace}/{DP_FOLDER}'
    return dp_dir
def GetDataFolder(workspace: str):
    dp_dir = f'{workspace}/{DATA_FOLDER}'
    return dp_dir

class ComputeModule:
    _private_constructor_key = uuid.uuid4().hex
    SAVE_FILE='module.json'
    ENTRY_POINT='entry_point.py'
    def __init__(self, root: str, inputs: list[str], outputs: list[str], _private_constructor_key=None) -> None:
        assert _private_constructor_key==self._private_constructor_key, f"constructor is private, use [ComputeModule.CreateNew(...)]"
        self._root = os.path.abspath(_remove_trailing_slash(root))
        self.name = root.split('/')[-1]
        self._inputs = inputs
        self._outputs = outputs

    def _test_run(self, workspace: str):
        workspace = _remove_trailing_slash(workspace)
        os.system(f'{self.GetEntryPoint()} {workspace}')

    def GetEntryPoint(self):
        return f'{sys.executable} {self._root}/{self.ENTRY_POINT}'

    def _get_inputs(self, workspace: str) -> list[DataPointer]:
        inputs = [DataPointer.LoadFromFile(workspace, dp_key) for dp_key in self._inputs]
        return inputs

    def _submit_outputs(self, outputs: list[DataPointer]):
        for dp in outputs:
            dp.SaveToDisk()

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

            return ComputeModule(
                folder_path, inputs, outputs,
                _private_constructor_key=cls._private_constructor_key
            )

    @classmethod
    def CreateNew(cls, save_location: str, name: str, inputs: set[str], outputs: set[str], overwrite: bool=False):
        save_location = _remove_trailing_slash(save_location)
        name = name.replace('/', '_').replace(' ', '-')
        root = f'{save_location}/{name}'
        ep = f'{root}/{cls.ENTRY_POINT}'

        if overwrite or not os.path.isdir(root):
            os.makedirs(root, exist_ok=True)
        else:
            raise FileExistsError(f"module [{name}] already exists at [{save_location}]")

        # if not os.path.isfile(ep):
        # with open(ep, 'w') as entry:
        #     entry.writelines([f'{line}\n' for line in [
        #         '#!/bin/bash',
        #         'PYTHONPATH=../../lib:${PYTHONPATH}',
        #         'SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )',
        #         'echo $@',
        #     ]])

        HERE = '/'.join(os.path.realpath(__file__).split('/')[:-1])
        templates = f'{HERE}/compute_module_template/'
        for template in os.listdir(templates):
            shutil.copy(f'{templates}/{template}', f'{root}/{template}')
        os.chmod(ep, 0o775)

        sf = f'{root}/{cls.SAVE_FILE}'
        if not os.path.isfile(sf):
            with open(sf, 'w') as save:
                save.write(json.dumps(dict(
                    io=dict(
                        inputs=list(inputs),
                        outputs=list(outputs)
                    )
                ), indent=4))

        return cls.LoadFromDisk(root)

        
