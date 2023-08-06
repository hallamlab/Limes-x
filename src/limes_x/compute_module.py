import os, sys
from typing import Callable, Any
from pathlib import Path
import importlib
import hashlib

from .models import Namespace, Dependency, Transform, KeyGenerator

# --------------------------------------------------
# meant for external use such as module definitions
# or starting workflows

def LazySet(properties: str):
    """@properties will be split using ', '"""
    return set(properties.split(", "))

def CreateTransform(file: str):
    fpath = Path(file)
    assert fpath.exists(), f"file not found [{file}]"
    contents = ""
    with open(file) as f:
        contents = "\n".join(f.readlines())
    hash = hashlib.md5(contents.encode(encoding="latin1"))
    tr = Transform(Namespace())
    tr.key = KeyGenerator(True).FromHex(hash.hexdigest(), l=16)
    return tr

# --------------------------------------------------

class ExecutionContext:
    def __init__(self, manifest: dict[Dependency, Any], output_folder: Path, shell: Callable[[str], bool]) -> None:
        self.manifest = manifest
        self.output_folder = output_folder
        self._shell = shell
    
    def Shell(self, cmd: str) -> bool:
        return self._shell(cmd)

class ComputeModule:
    def __init__(self, definition: str|Path):
        definition = Path(os.path.abspath(definition))
        assert os.path.exists(definition), f"path [{definition}] doesn't exist"
        assert os.path.isfile(definition), f"definition at [{definition}] isn't a file"
        self.name = definition.name
        SUFFIX = ".py"
        assert self.name.endswith(SUFFIX), f"definition at [{self.name}] doesn't end with [{SUFFIX}]"
        self.name = self.name[:-len(SUFFIX)]

        folder_path = definition.parent
        original_path = sys.path
        sys.path = [str(folder_path)]+sys.path
        try:
            mo = __import__(f"{self.name}")
            # import self.name as mo # type: ignore
            # importlib.reload(mo)

            ATTRIBUTES = ["ME", "Procedure"]
            for a in ATTRIBUTES:
                assert hasattr(mo, a), f"global variable [{a}] not found in module definition"

            self.transform: Transform = mo.ME
            self.procedure: Callable[[ExecutionContext], dict[str, Any]] = mo.Procedure
            self.location = folder_path
        except ImportError as e:
            # raise ImportError(f"failed to import [{self.name}] at [{folder_path}]")
            raise ImportError(e)
        finally:
            sys.path = original_path

    def __repr__(self) -> str:
        return f'<m:{self.name}>'

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, ComputeModule): return False
        return self.name == __o.name
    
    def ToDict(self):
        return dict(
            # name = self.name,
            path = str(self.location),
        )
    @classmethod
    def FromDict(cls, data: dict):
        return ComputeModule(data["path"])

    def Execute(self, context: ExecutionContext):
        return self.procedure(context)
