from __future__ import annotations
import os, sys
from pathlib import Path
import shutil
import importlib
from typing import Callable, Iterable, Any, Literal
import json
import inspect

# from .solver import Transform
from .common.utils import AutoPopulate, PrivateInit

# different instances can have differing "group_by"
# depending on parent compute module!
# 
# Items really shouldn't have group by
# this is a property of the compute module's input...
# 
# ItemInstances use the string key to circumvent
# possible incosistencies due to this
class Item:
    _hashes: dict[str, int] = {}
    _last_hash = 0

    def __repr__(self) -> str:
        return f'<i:{self.key}>'

    def __init__(self, key: str, group_by: Item|None = None) -> None:
        self.key = key
        self.group_by = group_by
        if key in Item._hashes:
            self._hash = Item._hashes[key]
        else:
            Item._last_hash += 1
            self._hash = Item._last_hash
            Item._hashes[key] = self._hash

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Item): return False
        return __o.key == self.key

    def __hash__(self) -> int:
        return self._hash

ManifestDict = dict[Item, Path]

class Params:
    def __init__(self,
        file_system_wait_sec: int=5,
        threads: int=4,
        mem_gb: int=8,
    ) -> None:
        self.file_system_wait_sec = file_system_wait_sec
        self.threads = threads
        self.mem_gb = mem_gb

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
        return p

class JobContext(AutoPopulate):
    __FILE_NAME = 'context.json'
    shell_prefix: str
    params: Params
    shell: Callable[[str], int]
    output_folder: Path
    manifest: dict[Item, Path|list[Path]]
    job_id: str
    run_before: str
    run_after: str

    def Save(self, workspace: Path):
        folder = workspace.joinpath(self.output_folder)
        if not folder.exists(): os.makedirs(folder)
        with open(folder.joinpath(JobContext.__FILE_NAME), 'w') as j:
            d = {}
            for k, v in self.__dict__.items():
                if k.startswith('_'): continue
                if k in ['shell', 'output_folder']: continue
                v: Any = v
                if v is None: continue
                v = { # switch
                    'shell': lambda: None,
                    'params': lambda: v.ToDict(),
                    'manifest': lambda: dict((mk.key, [str(p) for p in mv] if isinstance(mv, list) else str(mv)) for mk, mv in v.items()),
                }.get(k, lambda: str(v))()
                d[k] = v
            json.dump(d, j, indent=4)
            return d

    @classmethod
    def LoadFromDisk(cls, output_folder: Path):
        with open(output_folder.joinpath(JobContext.__FILE_NAME)) as j:
            d = json.load(j)
            kwargs = {}
            for k in d:
                v: Any = d[k]
                v = { # switch
                    'shell': lambda: None,
                    'params': lambda: Params.FromDict(v),
                    'output_folder': lambda: Path(v),
                    'manifest': lambda: dict((Item(mk), [Path(p) for p in mv] if isinstance(mv, list) else Path(mv)) for mk, mv in v.items()),
                }.get(k, lambda: str(v))()
                kwargs[k] = v
            if 'shell_prefix' not in d:
                kwargs['shell_prefix'] = ''
            if 'output_folder' not in d:
                kwargs['output_folder'] = output_folder
            return JobContext(**kwargs)

class JobResult(AutoPopulate):
    cmds: list[str]
    error_message: str|None
    made_by: str
    manifest: dict[Item, Path|list[Path]]
    resource_log: list[str]
    err_log: list[str]
    out_log: list[str]

    def ToDict(self):
        d = {}
        for k, v in self.__dict__.items():
            v: Any = v
            if v is None: continue
            v = { # switch
                "manifest": lambda: dict((mk.key, [str(p) for p in mv] if isinstance(mv, list) else str(mv)) for mk, mv in v.items()),
            }.get(k, lambda: v)()
            d[k] = v
        return d

    @classmethod
    def FromDict(cls, d: dict):
        man_dict = lambda v: dict((Item(mk), [Path(p) for p in mv] if isinstance(mv, list) else Path(mv)) for mk, mv in v.items())
        kwargs = {}
        for k in d:
            v: Any = d[k]
            v = { # switch
                "exit_code": lambda: int(v),
                "manifest": lambda: man_dict(v) if isinstance(v, dict) else [man_dict(x) for x in v],
            }.get(k, lambda: v)()
            kwargs[k] = v
        return JobResult(**kwargs)

class ModuleExistsError(FileExistsError):
    pass

class ComputeModule(PrivateInit):
    def __init__(self,
        procedure: Callable[[JobContext], JobResult],
        inputs: set[Item],
        outputs: set[Item],
        location: str|Path,
        name: str|None = None,
        **kwargs
    ) -> None:

        super().__init__(_key=kwargs.get('_key'))
        self.name = procedure.__name__ if name is None else name
        assert self.name != ""
        assert len(inputs.intersection(outputs)) == 0
        self.inputs = inputs
        self.outputs = outputs
        self._procedure = procedure
        self.location = Path(location).absolute()
        self.output_mask: set[Item] = set()

    @classmethod
    def LoadSet(cls, modules_path: str|Path):
        modules_path = Path(modules_path)
        compute_modules = []
        for dir in os.listdir(modules_path):
            mpath = modules_path.joinpath(dir)
            if not os.path.isdir(mpath): continue
            try:
                m = ComputeModule._load(mpath)
                compute_modules.append(m)
            except AssertionError:
                print(f"[{dir}] failed to load")
                continue
        return compute_modules

    @classmethod
    def _load(cls, folder_path: str|Path):
        folder_path = Path(os.path.abspath(folder_path))
        name = str(folder_path).split('/')[-1] # the folder name

        err_msg = f"module [{name}] at [{folder_path}] appears to be corrupted"
        assert os.path.exists(folder_path), err_msg
        assert os.path.isfile(folder_path.joinpath("definition.py")), err_msg
        # assert os.path.isfile(folder_path.joinpath("__main__.py")), err_msg
        original_path = sys.path
        # os.chdir(folder_path.joinpath('..'))
        sys.path = [str(folder_path)]+sys.path
        try:
            import definition as mo # type: ignore
            importlib.reload(mo)

            _procdure, ins, outs = mo.Procedure, mo.INPUTS, mo.OUTPUTS
            assert callable(_procdure)
            module = ComputeModule(
                _procdure,
                set(ins), set(outs),
                location=folder_path,
                name=name,
                _key=cls._initializer_key
            )

            return module
        except ImportError:
            raise ImportError(err_msg)
        finally:
            sys.path = original_path

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
                return cls._load(module_root)

        try:
            HERE = '/'.join(os.path.realpath(__file__).split('/')[:-1])
        except NameError:
            HERE = os.getcwd()
        templates = f'{HERE}/compute_module_template/'
        shutil.copytree(templates, module_root, ignore=shutil.ignore_patterns("__*__.py", "__pycache__", ".*"))
        for path, dirs, files in os.walk(module_root):
            for f in files:
                os.chmod(os.path.join(path, f), 0o775)

        return cls._load(module_root)

    def __repr__(self) -> str:
        return f'<m:{self.name}>'

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, ComputeModule): return False
        return self.name == __o.name

    def MaskOutput(self, item: Item):
        if item in self.outputs: self.output_mask.add(item)

    def GetUnmaskedOutputs(self):
        return self.outputs - self.output_mask

    def GenerateStaticRunCommand(self, workspace: Path, output_folder: Path):
        import metaworkflow
        from metaworkflow import compute_module_template

        def _get_path(mod):
            return os.path.abspath(os.path.dirname(inspect.getfile(mod)))

        py_path = "/".join(_get_path(metaworkflow).split("/")[:-1])
        entry_path = Path(_get_path(compute_module_template)).joinpath("__main__.py")
        return f'python {entry_path} {self.location} {workspace} {output_folder} {py_path}'
