#import explicitly, as entrypoint not present during test  (only after setup.py and dist file created)
#from pytest_mproc.plugin import *  # noqa
import pytest

@pytest.fixture(scope='global')
def dummy():
    pass


class V:
    value = 41


@pytest.fixture(scope='global')
def global_fix(dummy):
    V.value += 1
    return V.value  # we will assert the fixture is 42 in tests and never increases, as this should only be called once
