from __future__ import annotations
from enum import Enum, auto
import asyncio
from typing import Any, Callable, Coroutine

def type_name(type: type):
    return f"{type}".split(".")[-1][:-2].lower()

class ACTION(Enum):
    ACKNOWLEDGE = auto()
    STUMPED = auto()
    CALL = auto()
    INFORM = auto()

class Message:
    def __init__(self, action: ACTION, source: Agent, meta: dict, value: Any) -> None:
        self.action = action
        self.source = source
        self.meta = meta
        self.value = value

    def __str__(self) -> str:
        return f"{self.source}:{self.action.name.split('.')[-1]}:{self.value}"

    def __repr__(self) -> str:
        return f"{self}"

    @classmethod
    def CallFunction(cls, src: Agent, fn: str, params: tuple[tuple, dict]):
        return Message(ACTION.CALL, src, dict(name=fn), params)
    
    @classmethod
    def Inform(cls, src: Agent, value: Any):
        return Message(ACTION.INFORM, src, {}, value)
    
    @classmethod
    def Acknowledge(cls, src: Agent):
        return Message(ACTION.ACKNOWLEDGE, src, {}, None)
    
    @classmethod
    def Stumped(cls, src: Agent, err:str="", additional: Any=None):
        meta = {}
        if err != "":
            meta["err"] = err
        return Message(ACTION.STUMPED, src, meta, additional)

class Agent:
    def __init__(self, home_base: Outpost) -> None:
        type_str = type_name(type(self))
        assert type_str != "agent", "class Agent is abstract"
        self._type = type_str
        self._home_base = home_base
        self._home_base.RegisterAgent(self)

        self._handlers: dict[ACTION, Callable] = {}
        async def call(msg: Message) -> Message:
            fn_name = msg.meta["name"]
            fail = lambda _: Message.Stumped(self)
            fn = getattr(self, fn_name, fail)
            _args, _kwargs = msg.value
            if asyncio.iscoroutinefunction(fn):
                result = await fn(*_args, **_kwargs)
            else:
                return fail(msg)
            if isinstance(result, Message):
                return result
            elif result is None:
                return Message.Acknowledge(self)
            else:
                return Message.Inform(self, result)
        self._handlers[ACTION.CALL] = call

    def __repr__(self) -> str:
        name = f"{type(self)}".split(".")[-1][:-2]
        return f"<{name}.{self.__hash__():0x}>"

    async def Converse(self, message: Message):
        action = message.action
        if action not in self._handlers:
            return Message.Stumped(self)
        
        fn = self._handlers[action]
        return await fn(message)
    
    def CreateOnDelegateCallback(self, get_candidates: Callable[[], list[Agent]], delegate_fn: str, err_no_agents: str):
        async def _delegate(*args, **kwargs):
            candidates = get_candidates()
            if len(candidates) == 0: return Message.Stumped(self, err_no_agents)
            returns = await asyncio.gather(
                *[agent.Converse(Message.CallFunction(self, delegate_fn, (args, kwargs))) for agent in candidates]
            )
            return Message.Inform(self, returns)
        return _delegate
    
    # def CreateOnDelegateCallback(self, get_candidates: Callable[[], list[Agent]], delegate_fn: str, err_no_agents: str):
    #     def _delegate(*args, **kwargs):
    #         candidates = get_candidates()
    #         if len(candidates) == 0: return Message.Stumped(err_no_agents)
    #         returns = []
    #         for agent in candidates:
    #             r = agent.Converse(Message.CallFunction(delegate_fn, (args, kwargs)))
    #             returns.append(r)
    #         return Message.Inform(returns)
    #     return _delegate

# central point of contact to rest of agents in the network
class Outpost:
    def __init__(self) -> None:
        self._agents: dict[str, list[Agent]] = {}

    def RegisterAgent(self, agent: Agent):
        k = agent._type
        self._agents[k] = self._agents.get(k, []) + [agent]

    def ListAgentTypes(self):
        return [k for k in self._agents]

    def GetAgent(self, type: str):
        return list(self._agents.get(type, []))
