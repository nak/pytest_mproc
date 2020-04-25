#import explicitly, as entrypoint not present during test  (only after setup.py and dist file created)
from pytest_mproc.plugin import *  # noqa
#import pytest

@pytest.fixture(scope='global')
def dummy():
    pass

@pytest.fixture(scope='global')
def global_fix(dummy):
    return 42

