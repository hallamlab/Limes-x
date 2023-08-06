import os, sys
from pathlib import Path
import time
import pytest

from limes_x.executor.models import Config, Message, Context, Plan
from limes_x.executor.service import Service
from limes_x.executor.comms import Connection, Api
from limes_x.executor.launcher import EnsureServer
from limes_x.compute_module import ComputeModule

from helpers import ConditionalSkip, GetHere
from limes_x.models import Namespace, Application, DataInstance, Endpoint

def _set(s: str):
    return set(s.split(", "))

HERE = GetHere(__file__)
CONFIG_LOCAL = Config.Load(HERE.joinpath("config.local.yml"))

SERVICE_LOCAL = Service(CONFIG_LOCAL)

@ConditionalSkip(__file__)
class Test_service:
    # https://docs.pytest.org/en/latest/how-to/xunit_setup.html
    @classmethod
    def teardown_class(cls):
        # shutdown local_service if ssh tunneling
        pass

    def test_ping(self):
        ECHO = "anything here is fine"
        res = SERVICE_LOCAL.Handle(Message(
            Context.PING, ECHO
        ))

        assert res.context == Context.RESPONSE
        assert res.payload == ECHO

    def test_bad_config(self):
        res = SERVICE_LOCAL.Handle(Message(
            Context.SET_CONFIG, []
        ))
        assert res.context == Context.ERROR
        assert isinstance(res.payload, str)
    
    def _get_mods(self):
        c = CONFIG_LOCAL
        l = [p.replace(".py", "") for g in [os.listdir(mpath) for mpath in c.compute_modules] for p in g if ".py" in p]
        return sorted(l)

    def test_set_config(self):
        res = SERVICE_LOCAL.Handle(Message(
            Context.SET_CONFIG, CONFIG_LOCAL
        ))
        assert res.context == Context.RESPONSE
        _, home, modules = res.payload
        assert home == CONFIG_LOCAL.home
        assert [n for n, t in modules] == self._get_mods()

    def test_list_transforms(self):
        res = SERVICE_LOCAL.Handle(Message(
            Context.LIST_TRANSFORMS
        ))
        assert res.context == Context.RESPONSE
        assert [n for n, t in res.payload] == self._get_mods()

    @pytest.fixture
    def ns(self):
        return Namespace()

    def test_run1(self, ns):
        c = CONFIG_LOCAL
        mod = None
        for mpath in c.compute_modules:
            if "simple" not in f"{mpath}": continue
            mod = ComputeModule(mpath.joinpath("chain_ab.py"))
        assert mod is not None

        data = c.home.joinpath("data")
        a_proto = Endpoint(ns, _set("a"))
        a = DataInstance(a_proto, data.joinpath("a"))
        b_proto = Endpoint(ns, _set("b"))

        plan = Plan(
            [a],
            [Application(
                mod.transform,
                used={a_proto: mod.transform.requires[0]},
                produced={b_proto: mod.transform.produces[0]}
            )]
        )

        res = SERVICE_LOCAL.Handle(Message(
            Context.REGISTER_PLAN, [plan]
        ))

        print(res)
        time.sleep(0.5)

        assert False
