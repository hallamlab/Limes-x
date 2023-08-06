import os
from pathlib import Path
import pytest

def GetHere(file):
    return Path("/".join(os.path.realpath(file).split('/')[:-1]))

# decorator
def ConditionalSkip(file):
    key = f"SKIP_{Path(file).name[len('test_'):-len('.py')].upper()}"
    def _decorated(obj):
        decorator = pytest.mark.skipif(key in os.environ, reason="disabled via environment variable")
        return decorator(obj)
    return _decorated
