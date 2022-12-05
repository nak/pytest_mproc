import os
import subprocess
import time
from contextlib import suppress
from pathlib import Path
from typing import List

import pytest_mproc.fixtures
from pytest_mproc.worker import WorkerAgent

if os.path.exists("../src/pytest_mproc"):
    import sys
    #from pytest_mproc.plugin import *  # noqa
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))
import multiprocessing
import pytest
from pytest_mproc.plugin import TmpDirFactory


my_ip = pytest_mproc._get_my_ip()


def pytest_addoption(parser):
    # noinspection PyProtectedMember
    if 'pytest_mproc' not in [g.name for g in parser._groups]:
        pytest_mproc.plugin.pytest_addoption(parser)


_node_tmpdir = None


def get_ip_addr():
    import socket
    hostname = socket.gethostname()
    # noinspection PyBroadException
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


# noinspection PyUnusedLocal,PyShadowingNames
@pytest_mproc.fixtures.global_fixture()
def global_fix(dummy):
    GlobalV.value += 1
    # we will assert the fixture is 42 in tests and never increases, as this should only be called once
    return GlobalV.value


@pytest_mproc.fixtures.node_fixture()
@pytest_mproc.group(pytest_mproc.GroupTag(name='node_fixture',
                                          restrict_to=pytest_mproc.data.TestExecutionConstraint.SINGLE_NODE))
def node_level_fixture(mp_tmp_dir_factory: TmpDirFactory):
    # noinspection PyProtectedMember
    _node_tmpdir_ = mp_tmp_dir_factory._root_tmp_dir
    assert os.path.exists(_node_tmpdir_)
    V.value += 1
    return V.value  # we will assert the fixture is 42 in tests and never increases, as this should only be called once


@pytest.mark.trylast
def pytest_sessionfinish(session):
    if _node_tmpdir and not hasattr(session.config.option, "mproc_worker"):
        # noinspection PyTypeChecker
        assert not os.path.exists(_node_tmpdir), f"Failed to cleanup temp dirs"


@pytest.fixture()
def chdir():
    cwd = os.getcwd()

    class ChDir:

        # noinspection PyMethodMayBeStatic
        def chdir(self, path: Path):
            os.chdir(path)

    yield ChDir()
    os.chdir(cwd)


class WorkerAgentFactory:

    def __init__(self):
        self._agents: List[WorkerAgent] = []
        self._procs: List[subprocess.Popen] = []

    # noinspection PyProtectedMember
    def create_worker_agent(self, port: int, authkey: bytes, separate_process: bool = False):
        if separate_process:
            proc = subprocess.Popen(f"{sys.executable} -m pytest_mproc.worker",
                                    cwd=os.getcwd(),
                                    env=os.environ,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE)
            proc.stdin.write(authkey.hex().encode('utf-8') + b'\n')
            proc.stdin.close()
            port = int(proc.stdout.read())
            agent = WorkerAgent.as_client(address=(my_ip, port), authkey=authkey)
            self._agents.append(agent)
            self._procs.append(proc)
            return agent
        else:
            agent = WorkerAgent.as_server(address=(my_ip, port), authkey=authkey)
            self._agents.append(agent)
            return agent

    def shutdown(self):
        for agent in self._agents:
            agent.shutdown()
        for proc in self._procs:
            with suppress(Exception):
                proc.wait(timeout=5)
            if proc.returncode is None:
                proc.terminate()


@pytest.fixture
def worker_agent_factory() -> WorkerAgentFactory:
    factory = WorkerAgentFactory()
    yield factory
    factory.shutdown()
