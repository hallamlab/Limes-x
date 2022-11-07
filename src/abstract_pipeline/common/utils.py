import os
import uuid
from typing import Callable
from inspect import signature

def RemoveTrailingSlash(path: str):
    return path[:-1] if path[-1] == '/' else path

class AbstractClassException(Exception):
    def __init__(self) -> None:
        super().__init__(f'can not initialize abstract class without concrete implimentation')

class AbstractFunctionException(Exception):
    def __init__(self) -> None:
        super().__init__(f'this abstract function has no implimentation')

class Abstract:
    """pass self._abstract_initializer_key in constructor of implimenting class"""
    _abstract_initializer_key: str = uuid.uuid4().hex

    def __init__(self, _key=None) -> None:
        if _key != self._abstract_initializer_key: raise AbstractClassException

###

class Overloader:
    """rudimentary, supposedly threadsafe, dispatch-style method overloading
    - register the class, then overload methods
    - disables type hints for overloaded functions
    - pylance indicates duplicate function names, use "# type: ignore" to ignore
    """

    def __init__(self) -> None:
        self._execute_later: list[Callable] = []

    def RegisterClass(self, cls):
        all_overloads: dict = {}
        for fn in self._execute_later:
            fn(all_overloads)

        def _make_dispatcher(fn_name, overloads):
            def _dispatcher(*args, **kwargs):
                for sig, candidate in overloads:
                    if len(args)+len(kwargs) > len(sig.parameters): continue
                    b = sig.bind_partial(*args, **kwargs)
                    return candidate(*b.args, **b.kwargs)
                raise TypeError(f"dispatcher for [{fn_name}] unable to find matching overload for [{args}] [{kwargs}]")
            return _dispatcher

        for fn_name, overloads in all_overloads.items():
            setattr(cls, fn_name, _make_dispatcher(fn_name, overloads))
        return cls

    def Overload(self, fn) -> Callable:
        def _later(all_overloads: dict):
            fn_name = fn.__name__
            fn_overloads = all_overloads.get(fn_name, [])
            fn_overloads.append((signature(fn), fn))
            # signature(fn).bind_partial().
            all_overloads[fn_name] = fn_overloads

        self._execute_later.append(_later)
        return fn

# # example

# o = Overloader()
# @o.WithOverloads
# class _X:
#     @o.Overload
#     def add(self, a: int, b): # type: ignore
#         print('int')
#         return a+b

#     @o.Overload
#     def add(self, a: float, b: float, c=False): # type: ignore
#         print('float')
#         return a+b

#     @o.Overload
#     def sub(self, a, b): # type: ignore
#         return a-b

#     @o.Overload
#     def sub(self, a, b, c): # type: ignore
#         return a-b-c

# x = _X()
# print(x.add(1, 2))
# print(x.sub(1, 2, 3))
