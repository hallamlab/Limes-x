import os

from typing import Any
from .models import Config, Context, Message, Plan
from .executor import Executor
from ..compute_module import ComputeModule

class Service:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.modules: list[ComputeModule] = []

        self.exe = Executor(config)
        self.exe.Start()

    def set_config(self, payload: Any):
        assert isinstance(payload, Config), f"expected Config, got [{type(payload)}]"
        
        config: Config = payload
        old_config = self.config
        self.config = config
        for cm_dir in self.config.compute_modules:
            if not (cm_dir.exists() and cm_dir.is_dir()):
                # print(f"{cm_dir} doesn't exist")
                return
            
            for mfile in os.listdir(cm_dir):
                mpath = cm_dir.joinpath(mfile)
                # print(mpath)
                try:
                    module = ComputeModule(mpath)
                    self.modules.append(module)
                except AssertionError as a:
                    continue
                except ImportError as e:
                    # malformed.append(mpa)
                    print(f"[{mfile}] was malformed")
        self.modules = sorted(self.modules, key=lambda m: m.name)

        print(f"config loaded, home: {config.home}")
        print(f"module folders:")
        for mpath in config.compute_modules:
            print(f"\t- {mpath}")
        print(f"compute modules:")
        _found = False
        for m in self.modules:
            print(f"\t- {m} {m.transform}")
            _found = True
        if not _found:
            print(f"\t- (no modules found)")
        return old_config.home, config.home, self.list_transforms()

    def ping(self, payload):
        return payload

    def list_transforms(self, payload=None):
        return [(m.name, m.transform) for m in self.modules]
    
    def register_plan(self, payload: list[Plan]):
        return self.exe.Register(payload)

    def Handle(self, msg: Message) -> Message:
        fn_name = msg.context.name.lower()
        if hasattr(self, fn_name):
            try:
                content = msg.payload
                result = getattr(self, fn_name)(content)
                return Message(Context.RESPONSE, result, msg.key)
            except (Exception, AssertionError) as e:
                return Message(Context.ERROR, f"error [{e}]", msg.key)

        return Message(Context.ERROR, ["I don't know what to do with this", msg.context, msg.payload], msg.key)