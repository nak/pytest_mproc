import os
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from typing import List

import pytest_mproc.fixtures
from pytest_mproc.orchestration import Orchestrator, MainSession
from pytest_mproc.worker import WorkerAgent

if os.path.exists("../src/pytest_mproc"):
    import sys
    #from pytest_mproc.plugin import *  # noqa
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))
import multiprocessing
import pytest
from pytest_mproc.plugin import TmpDirFactory
from pytest_mproc.fixtures import Global, Node


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


@pytest.hookimpl(trylast=True)
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
            agent = WorkerAgent.start_server(address=(my_ip, port), authkey=authkey)
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


shielded = os.environ.get("SHIELDED", False)


@pytest.fixture(scope='session')
def shield_always():
    global shielded
    shielded = True

@pytest.fixture
def shield():
    """
    we are testing pytest_mproc and run pytest.main against resources/project_tests in some cases.  When doing this,
    we must shield the cleanup fixture from running against those tsts since they may pick up this contest.py file
    """
    global shielded
    shielded = True
    yield
    shielded = False


@pytest.fixture(autouse=True)
def cleanup():
    global shielded
    yield

    if not shielded:
        Orchestrator._mp_server = None
        Orchestrator._mp_client = None
        Orchestrator._registered = False
        Orchestrator._instances = {}
        MainSession._singleton = None
        WorkerAgent._mp_server = None
        WorkerAgent._mp_client = None
        WorkerAgent._registered = False
        WorkerAgent._sessions = {}
        WorkerAgent._instances = {}
        WorkerAgent._node_mgrs = {}
        if Global.Manager.singleton():
            Global.Manager.singleton().stop()
