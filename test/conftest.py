#import explicitly, as entrypoint not present during test  (only after setup.py and dist file created)
#from pytest_mproc.plugin import *  # noqa
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))
import multiprocessing
import pytest
print(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> {os.environ.get('PYTHONPATH')}")
from pytest_mproc.plugin import TmpDirFactory


_node_tmpdir = None


@pytest.fixture(scope='global')
def dummy():
    return [None]


@pytest.fixture(scope='node')
def queue_fixture():
    # cannot be shared at global level, so must be node-scoped
    # (will raise  mnultiprocessing.context.AuthenticationError: digest sent was rejected
    #  when sharing queue across machines)
    global _node_tmpdir
    m = multiprocessing.Manager()
    return m.Queue()


class V:
    value = 41


class GlobalV:
    value = 41


@pytest.fixture(scope='global')
def global_fix(dummy):
    GlobalV.value += 1
    return GlobalV.value  # we will assert the fixture is 42 in tests and never increases, as this should only be called once


import pytest_mproc
@pytest.fixture(scope='node')
@pytest_mproc.group(pytest_mproc.GroupTag(name='node_fixture', restrict_to=pytest_mproc.data.TestExecutionConstraint.SINGLE_NODE))
def node_level_fixture(mp_tmp_dir_factory: TmpDirFactory):
    _node_tmpdir = mp_tmp_dir_factory._root_tmp_dir
    assert os.path.exists(_node_tmpdir)
    V.value += 1
    return V.value  # we will assert the fixture is 42 in tests and never increases, as this should only be called once


@pytest.mark.trylast
def pytest_sessionfinish(session):
    if _node_tmpdir and not hasattr(session.config.option, "mproc_worker"):
       assert not os.path.exists(_node_tmpdir), f"Failed to cleanup temp dirs"
