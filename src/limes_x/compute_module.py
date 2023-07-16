import os, sys
from typing import Callable, Any
from pathlib import Path
import importlib

from .models import Namespace, Dependency, Transform

# --------------------------------------------------
# meant for external use such as module definitions
# or starting workflows

def LazySet(properties: str):
    """@properties will be split using ', '"""
    return set(properties.split(", "))

def CreateTransform():
    return Transform(Namespace())
# --------------------------------------------------

class ExecutionContext:
    def __init__(self, manifest: dict[Dependency, Any], output_folder: Path, shell: Callable[[str], bool]) -> None:
        self.manifest = manifest
        self.output_folder = output_folder
        self._shell = shell
    
    def Shell(self, cmd: str) -> bool:
        return self._shell(cmd)

class ComputeModule:
    _DEFINITION_FILE_NAME = 'definition.py'
    def __init__(self, folder_path: str|Path):
        folder_path = Path(os.path.abspath(folder_path))
        self.name = folder_path.name
        assert os.path.exists(folder_path), f"path [{folder_path}] doesn't exist"
        def_file = folder_path.joinpath(f'{self._DEFINITION_FILE_NAME}')
        assert def_file.exists(), f"def. file at {def_file} doesn't exist"
        assert os.path.isfile(def_file), f"def file at {def_file} isn't file"

        original_path = sys.path
        sys.path = [str(folder_path)]+sys.path
        try:
            import definition as mo # type: ignore
            importlib.reload(mo)

            ATTRIBUTES = ["ME", "Procedure"]
            for a in ATTRIBUTES:
                assert hasattr(mo, a), f"global variable [{a}] not found in module definition"

            self.transform: Transform = mo.ME
            self.procedure: Callable[[ExecutionContext], dict[str, Any]] = mo.Procedure
            self.location = folder_path
        except ImportError:
            raise ImportError(f"failed to import [{self.name}] at [{folder_path}]")
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
