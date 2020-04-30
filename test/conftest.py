#import explicitly, as entrypoint not present during test  (only after setup.py and dist file created)
#from pytest_mproc.plugin import *  # noqa
import os

import pytest

from pytest_mproc.plugin import TmpDirFactory


@pytest.fixture(scope='global')
def dummy():
    pass


class V:
    value = 41


@pytest.fixture(scope='global')
def global_fix(dummy):
    V.value += 1
    return V.value  # we will assert the fixture is 42 in tests and never increases, as this should only be called once


@pytest.fixture(scope='node')
def node_level_fixture():
    V.value += 1
    return V.value  # we will assert the fixture is 42 in tests and never increases, as this should only be called once


@pytest.mark.trylast
def pytest_sessionfinish(session):
    if not hasattr(session.config.option, "mproc_worker"):
        assert not os.path.exists(TmpDirFactory._root_tmp_dir), f"Failed to cleanup temp dirs"
