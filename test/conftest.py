#import explicitly, as entrypoint not present during test  (only after setup.py and dist file created)
import os
from pathlib import Path

import pytest_mproc.fixtures

if os.path.exists("../src/pytest_mproc"):
    import sys
    #from pytest_mproc.plugin import *  # noqa
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))
import multiprocessing
import pytest
from pytest_mproc.plugin import TmpDirFactory


def pytest_addoption(parser):
    if 'pytest_mproc' not in [g.name for g in parser._groups]:
        pytest_mproc.plugin.pytest_addoption(parser)


_node_tmpdir = None

#pytest_mproc.Settings.set_ssh_credentials('pi')
#pytest_mproc.Settings.set_tmp_root(Path('/home/pi/tmp'))
#pytest_mproc.Settings.set_cache_dir(Path('/home/pi/cache'))


def get_ip_addr():
    import socket
    hostname = socket.gethostname()
    try:
        return socket.gethostbyname(hostname)
    except Exception:
        return None


@pytest_mproc.fixtures.global_fixture()
def dummy():
    return [None]


@pytest_mproc.fixtures.node_fixture()
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


@pytest_mproc.fixtures.global_fixture()
def global_fix(dummy):
    GlobalV.value += 1
    return GlobalV.value  # we will assert the fixture is 42 in tests and never increases, as this should only be called once


import pytest_mproc
@pytest_mproc.fixtures.node_fixture()
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


@pytest.fixture()
def chdir():
    cwd = os.getcwd()

    class ChDir:

        def chdir(self, path: Path):
            os.chdir(path)

    yield ChDir()
    os.chdir(cwd)