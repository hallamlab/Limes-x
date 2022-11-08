import re
import sys, os
import json
import uuid
import shutil
from .constants import MANIFESTS_FOLDER, DATA_FOLDER
from .common.utils import RemoveTrailingSlash
from data_pointer import ManifestTemplate, Manifest

def GetDataPointerFolder(workspace: str):
    dp_dir = f'{workspace}/{MANIFESTS_FOLDER}'
    return dp_dir
def GetDataFolder(workspace: str):
    dp_dir = f'{workspace}/{DATA_FOLDER}'
    return dp_dir

class ModuleExistsError(FileExistsError):
    pass

class ComputeModule:
    _private_constructor_key = uuid.uuid4().hex
    SAVE_FILE='module.json'
    ENTRY_POINT='entry_point.py'
    def __init__(self, root: str, inputs: list[ManifestTemplate], outputs: list[ManifestTemplate], _private_constructor_key=None) -> None:
        assert _private_constructor_key==self._private_constructor_key, f"constructor is private, use [ComputeModule.CreateNew(...)]"
        assert len(set(t.name for t in inputs)) == len(inputs), f"duplicate names for templates are not allowed: inputs [{inputs}]"
        assert len(set(t.name for t in outputs)) == len(outputs), f"duplicate names for templates are not allowed: outputs [{outputs}]"
        
        self._root = os.path.abspath(RemoveTrailingSlash(root))
        self.name = root.split('/')[-1]
        self._inputs = inputs
        self._outputs = outputs

    def __str__(self) -> str:
        return f'module: {self.name}'

    def __repr__(self) -> str:
        return self.__str__()

    def _test_run(self, workspace: str):
        workspace = RemoveTrailingSlash(workspace)
        os.system(f'{self.GetEntryPointCmd()} {workspace}')

    def GetEntryPointCmd(self):
        return f'{sys.executable} {self._root}/{self.ENTRY_POINT}'

    def GetInputManifests(self, workspace: str) -> dict[str, Manifest]:
        workspace = RemoveTrailingSlash(workspace)
        man_dir = f'{workspace}/{MANIFESTS_FOLDER}'
        return dict([(t.name, t.LoadManifest(f'{man_dir}/{t.GenerateSaveName()}')) for t in self._inputs])

    def GetOutputTemplates(self):
        return dict((t.name, t) for t in self._outputs)

    def RegisterOutput(self, workspace: str, manifest: Manifest):
        _candidate_ts = [t for t in self._outputs if t == manifest._template]
        assert len(_candidate_ts) == 1, f'template not in known outputs, use ListOutputTemplates() get options'
        workspace = RemoveTrailingSlash(workspace)
        manifests_dir = f'{workspace}/{MANIFESTS_FOLDER}'
        manifest.Save(manifests_dir)

    @classmethod
    def LoadFromDisk(cls, folder_path: str):
        save_path = f'{folder_path}/{cls.SAVE_FILE}'
        assert os.path.isfile(save_path), f"no [{cls.SAVE_FILE}] found at [{folder_path}]"
        assert os.path.isfile(f'{folder_path}/{cls.ENTRY_POINT}'), f"no [{cls.ENTRY_POINT}] found at [{folder_path}]"
        
        with open(save_path) as save:
            save_data = json.load(save)
            io = save_data['io']
            _load_template = lambda x: [ManifestTemplate(k, v) for k, v in io[x].items()]
            inputs = _load_template('inputs')
            outputs = _load_template('outputs')

            return ComputeModule(
                folder_path, inputs, outputs,
                _private_constructor_key=cls._private_constructor_key
            )

    @classmethod
    def CreateNew(cls, 
        save_location: str, name: str,
        inputs: list[ManifestTemplate], outputs: list[ManifestTemplate],
        on_exist: str='error'):

        save_location = RemoveTrailingSlash(save_location)
        name = name.replace('/', '_').replace(' ', '-')
        module_root = f'{save_location}/{name}'
        ep = f'{module_root}/{cls.ENTRY_POINT}'

        if os.path.exists(module_root):
            if on_exist=='overwrite':
                shutil.rmtree('module_root', ignore_errors=True)
            elif on_exist=='error':
                raise ModuleExistsError(f"module [{name}] already exists at [{save_location}]")
            elif on_exist=='ignore':
                print(f'module [{name}] already exits! ignoring...')
                return cls.LoadFromDisk(module_root)

        HERE = '/'.join(os.path.realpath(__file__).split('/')[:-1])
        templates = f'{HERE}/compute_module_template/'
        shutil.copytree(templates, module_root)
        os.chmod(ep, 0o775)

        sf = f'{module_root}/{cls.SAVE_FILE}'
        if not os.path.isfile(sf):
            with open(sf, 'w') as save:
                _save_template = lambda x: dict([d.GenerateDictEntry() for d in x])
                save.write(json.dumps(dict(
                    io=dict(
                        inputs=_save_template(inputs),
                        outputs=_save_template(outputs),
                    )
                ), indent=4))

        return cls.LoadFromDisk(module_root)
