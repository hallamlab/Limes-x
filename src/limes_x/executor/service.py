import os

from typing import Any
from .models import Config, Context, Message
from ..compute_module import ComputeModule

class Service:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.modules: list[ComputeModule] = []

    def set_config(self, payload: Any):
        if not isinstance(payload, Config):
            return f"expected Config, got [{type(payload)}]", Context.ERROR
        
        config: Config = payload
        old_config = self.config
        self.config = config
        if config.home != old_config.home:
            cm_dir = self.config.home.joinpath("compute_modules")
            if not (cm_dir.exists() and cm_dir.is_dir()):
                # print(f"{cm_dir} doesn't exist")
                return
            
            for dir in os.listdir(cm_dir):
                mpath = cm_dir.joinpath(dir)
                # print(mpath)
                try:
                    module = ComputeModule(mpath)
                    self.modules.append(module)
                except AssertionError as a:
                    continue

        print(f"config loaded [{config.home}]")
        print(f"compute modules:")
        for m in self.modules:
            print(f"\t- {m} {m.transform}")

    def list_transforms(self, payload: Any):
        return [m.transform for m in self.modules]

    def Handle(self, msg: Message) -> Message:
        fn_name = msg.context.name.lower()
        if hasattr(self, fn_name):
            try:
                content = msg.payload
                r = getattr(self, fn_name)(content)
                if isinstance(r, tuple) and len(r) == 2:
                    result, r_context = r
                else:
                    result, r_context = r, Context.RESPONSE
                return Message(r_context, result, msg.key)
            except Exception as e:
                return Message(Context.ERROR, f"error [{e}]", msg.key)

        return Message(Context.ERROR, ["I don't know what to do with this", msg.context, msg.payload], msg.key)