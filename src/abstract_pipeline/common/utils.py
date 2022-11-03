import uuid

def _remove_trailing_slash(path: str):
    return path[:-1] if path[-1] == '/' else path

class AbstractClassException(Exception):
    def __init__(self) -> None:
        super().__init__(f'can not initialize abstract class without concrete implimentation')

class AbstractFunctionException(Exception):
    def __init__(self) -> None:
        super().__init__(f'this abstract function has no implimentation')

# pass self._abstract_initializer_key in constructor of implimenting class
class Abstract:
    _abstract_initializer_key: str = uuid.uuid4().hex

    def __init__(self, _key=None) -> None:
        if _key != self._abstract_initializer_key: raise AbstractClassException