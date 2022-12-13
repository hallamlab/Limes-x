from __future__ import annotations
import os, sys
from pathlib import Path
import shutil
import importlib
from typing import Callable, Iterable, Any, Literal

from .solver import Transform
from .common.utils import AutoPopulate, PrivateInit

# each item should be its own atomic thing... [thinking emoji]
class Item(PrivateInit):
    _instances: dict[str, Item] = {}
    
    @classmethod
    def Get(cls, key: str) -> Item:
        if key not in cls._instances:
            instance = Item(key, _ikey=cls._initializer_key)
            cls._instances[key] = instance
        return cls._instances[key]

    def __repr__(self) -> str:
        return f'<i:{self.key}>'

    def __init__(self, key: str, **kwargs) -> None:
        super().__init__(_key=kwargs.get("_ikey"))
        self.key = key
        # self.persona_keys = {t.key for t in personas}

    def GenerateDictEntry(self):
        return self.key

    # def CanBe(self, other: Item):
    #     return self.key in other.persona_keys

    # def GenerateFilePath(self, root: Path, file: Path):
    #     # return [root.joinpath(f) for f in files]
    #     return root.joinpath(file)

ManifestDict = dict[Item, Path]

class Params(AutoPopulate):
    # id: str
    threads: int
    mem_gb: int

    def Copy(self):
        cp = Params(**self.__dict__)
        return cp

    def ToDict(self):
        return self.__dict__

    @classmethod
    def FromDict(cls, d: dict):
        p = Params()
        for k, v in d.items():
            setattr(p, k, v)

class RunContext(AutoPopulate):
    params: Params
    shell: Callable[[str], int]
    output_folder: Path
    manifest: ManifestDict

    def ToDict(self):
        d = {}
        for k, v in self.__dict__.items():
            if k in ['shell', 'workspace', 'output_folder']: continue
            v: Any = v
            v = { # switch
                Params: lambda: v.ToDict(),
                dict: lambda: dict((mk.key, str(mv)) for mk, mv in v.items()),
            }.get(type(v), lambda: str(v))()
            d[k] = v
        return d

    @classmethod
    def FromDict(cls, d: dict):
        kwargs = {}
        for k in d:
            v: Any = d[k]
            v = { # switch
                'shell': lambda: None,
                'params': lambda: Params.FromDict(v),
                'manifest': lambda: dict((Item.Get(mk), Path(mv)) for mk, mv in v.items()),
            }.get(k, lambda: Path(v))()
            kwargs[k] = v
        return RunContext(**kwargs)

class RunResult(AutoPopulate):
    exit_code: int
    manifest: ManifestDict|list[ManifestDict]

    def ToDict(self):
        d = {}
        for k, v in self.__dict__.items():
            v: Any = v
            v = { # switch
                int: lambda: v,
                dict: lambda: dict((mk.key, str(mv)) for mk, mv in v.items()),
            }.get(type(v), lambda: str(v))()
            d[k] = v
        return d

    @classmethod
    def FromDict(cls, d: dict):
        man_dict = lambda v: dict((Item.Get(mk), Path(mv)) for mk, mv in v.items())
        kwargs = {}
        for k in d:
            v: Any = d[k]
            v = { # switch
                "exit_code": lambda: int(v),
                "manifest": lambda: man_dict(v) if isinstance(v, dict) else [man_dict(x) for x in v],
            }.get(k, lambda: v)()
            kwargs[k] = v
        return RunResult(**kwargs)

class ModuleExistsError(FileExistsError):
    pass

class ComputeModule(PrivateInit):
    def __init__(self,
        procedure: Callable[[RunContext], RunResult],
        inputs: set[Item],
        outputs: set[Item],
        location: str|Path,
        name: str|None = None,
        **kwargs
    ) -> None:

        super().__init__(_key=kwargs.get('_key'))
        self.name = procedure.__name__ if name is None else name
        assert self.name != ""
        assert not Transform.Exists(self.name), f"duplicate compute module [{self.name}]"
        assert len(inputs.intersection(outputs)) == 0
        self.inputs = inputs
        self.outputs = outputs
        self._procedure = procedure
        self.location = Path(location).absolute()

    @classmethod
    def LoadFromDisk(cls, folder_path: str|Path):
        folder_path = str(Path(folder_path))
        assert os.path.exists(folder_path)
        original_paths = sys.path.copy()
        sys.path.insert(0, folder_path)
        de = importlib.import_module("definition", folder_path)

        _procdure, ins, outs = de.Procedure, de.INPUTS, de.OUTPUTS
        module = ComputeModule(
            _procdure,
            ins, outs,
            location=folder_path,
            name=folder_path.split('/')[-1], # the folder name
            _key=cls._initializer_key
        )

        sys.path = original_paths
        return module

    @classmethod
    def GenerateTemplate(cls, 
        modules_folder: str|Path,
        name: str,
        on_exist: Literal['error']|Literal['overwrite']|Literal['skip']='error'):
        modules_folder = Path(modules_folder)

        name = name.replace('/', '_').replace(' ', '-')
        module_root = Path.joinpath(modules_folder, name)

        if os.path.exists(module_root):
            if on_exist=='overwrite':
                shutil.rmtree(module_root, ignore_errors=True)
            elif on_exist=='error':
                raise ModuleExistsError(f"module [{name}] already exists at [{modules_folder}]")
            elif on_exist=='skip':
                print(f'module [{name}] already exits! skipping...')
                return cls.LoadFromDisk(module_root)

        try:
            HERE = '/'.join(os.path.realpath(__file__).split('/')[:-1])
        except NameError:
            HERE = os.getcwd()
        templates = f'{HERE}/compute_module_template/'
        shutil.copytree(templates, module_root)
        for path, dirs, files in os.walk(module_root):
            for f in files:
                os.chmod(os.path.join(path, f), 0o775)

        return cls.LoadFromDisk(module_root)

    def __repr__(self) -> str:
        return f'<m:{self.name}>'

    def GetTransform(self):
        if Transform.Exists(self.name):
            return Transform.Get(self.name)
        else:
            return Transform.Create(
                {x.key for x in self.inputs},
                {x.key for x in self.outputs},
                unique_name=self.name,
                reference=self,
            )

    def GenerateStaticRunCommand(self, workspace: Path, output_folder: Path):
        return f'python {self.location.joinpath("__main__.py")} {workspace} {output_folder}'
