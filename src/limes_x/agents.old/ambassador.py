from pathlib import Path
from typing import Any

from .generic import Outpost, Agent, type_name, Message, ACTION

# acts as interface to the network
class Ambassador(Agent):
    def __init__(self, home_base: Outpost) -> None:
        super().__init__(home_base)

        def _clerks():
            return self._home_base.GetAgent("clerk")
        def _data_fn(name):
            return self.CreateOnDelegateCallback(
                _clerks, name, "no datastores",
            )
        self.RegisterData = _data_fn("RegisterData")
        self.Query = _data_fn("Query")
        self.RegisterComputeModule = _data_fn("RegisterComputeModule")

