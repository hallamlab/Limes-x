import os, sys
import limes_x as lx

lx.out

# https://docs.pytest.org/en/latest/how-to/xunit_setup.html
class Test_executor:
    @classmethod
    def setup_class(cls):
        global x
        x = 1

    @classmethod
    def teardown_class(cls):
        global x
        x = 0

    def test_mock(self):
        global x
        assert x == 1
        x = 2

    def test_mock2(self):
        assert x == 2
