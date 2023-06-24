from typing import Callable, Any

from .generic import Outpost, Agent, type_name, Message, ACTION

class Dispatcher(Agent):
    def __init__(self, home_base: Outpost) -> None:
        super().__init__(home_base)