import multiprocessing
import os
import secrets
import shutil
import signal
import time
from contextlib import suppress
from typing import Optional, Tuple

from pytest_mproc.mp import JoinableQueue
from pathlib import Path
from unittest.mock import patch

import pytest
from _pytest.reports import TestReport

# noinspection PyProtectedMember
from pytest_mproc import find_free_port, _get_my_ip
from pytest_mproc.data import ReportStarted, ReportFinished, WorkerExited, AllWorkersDone
from pytest_mproc.orchestration import Orchestrator
from pytest_mproc.user_output import debug_print, always_print

TEST_PROXY_DIR = Path(__file__).parent / 'resources' / 'project_tests'


def test_start_globals():
    authkey = secrets.token_bytes(64)
    orchestrator = Orchestrator.as_server(address=('localhost', find_free_port()), authkey=authkey)
    data = orchestrator.start_globals()
    assert data is not None


# noinspection PyUnusedLocal
def mock_launch(session_id: str, authkey: bytes, main_address: Optional[Tuple[int, str]],
                main_authkey: Optional[bytes], test_q: JoinableQueue, status_q: JoinableQueue,
                report_q: JoinableQueue,
                worker_q: JoinableQueue, args, cwd,
                node_mgr_port: int):
    worker = worker_q.get()
    while worker is not None:
        time.sleep(1)
        worker = worker_q.get()


@patch("pytest_mproc.orchestration.MainSession.launch", new=mock_launch)
def test_start_session():
    authkey = secrets.token_bytes(64)
    port = find_free_port()
    server = Orchestrator.as_server(address=('localhost', port), authkey=authkey)
    server.start_globals()
    time.sleep(1)
    # noinspection PyUnresolvedReferences
    client = Orchestrator.as_client(address=('localhost', port), authkey=authkey, )
    client.conduct_session(session_id="Session1", args=['-s'], cwd=Path(os.getcwd()))
    assert list(server.sessions().copy()) == ['Session1']
    client.conduct_session(session_id="Session2", args=['-s'], cwd=Path(os.getcwd()))
    assert server.sessions().copy() == ['Session1', 'Session2']
    with suppress(ValueError):
        client.conduct_session(session_id="Session2", args=['-s'], cwd=Path(os.getcwd()))
    client.shutdown_session(session_id="Session1", timeout=5)
    assert server.sessions().copy() == ['Session2']
    client.shutdown_session(session_id="Session2", timeout=5)
    assert not server.sessions().copy()
    server.shutdown()


def test_start_session_nonlocal():
    authkey = secrets.token_bytes(64)
    port = find_free_port()
    orch_server = Orchestrator.as_server(address=('localhost', port), authkey=authkey)
    orch_server.start_globals()
    # noinspection PyUnresolvedReferences

    def run():
        del os.environ['PYTHONPATH']
        import sys
        for path in sys.path.copy():
            if Path(path).absolute() == Path(__file__).parent.absolute():
                sys.path.remove(path)
            if Path(path).absolute() == Path(__file__).parent.parent.absolute():
                sys.path.remove(path)
        os.environ['PYTHONPATH'] = str(Path(__file__).parent.parent / 'src')
        client = Orchestrator.as_client(address=('localhost', port), authkey=authkey)
        client.conduct_session(session_id="Session1", args=['-s', '--cores', '10',
                                                          f'--ignore={str(Path(__file__).parent)}'], cwd=TEST_PROXY_DIR)
        assert list(client.sessions().copy()) == ['Session1']
        client.conduct_session(session_id="Session2", args=['-s', '--cores', '10',
                                                          f'--ignore={str(Path(__file__).parent)}'
                                                            ], cwd=TEST_PROXY_DIR)
        assert client.sessions().copy() == ['Session1', 'Session2']
        with suppress(ValueError):
            client.conduct_session(session_id="Session2", args=[
                '-s', '--cores', '10', f'--ignore={str(Path(__file__).parent)}'], cwd=TEST_PROXY_DIR)
        status = client.shutdown_session(session_id="Session1", timeout=5)
        assert status['exitcode'] == -signal.SIGTERM
        assert client.sessions().copy() == ['Session2']
        status = client.shutdown_session(session_id="Session2", timeout=5)
        assert status['exitcode'] == -signal.SIGTERM
        assert not client.sessions().copy()

    try:
        proc = multiprocessing.Process(target=run)
        proc.start()
        proc.join()
        assert proc.exitcode == 0
    finally:
        orch_server.shutdown()


# noinspection PyUnusedLocal
def test_session_with_workers(worker_agent_factory, request, shield):
    authkey = secrets.token_bytes(64)
    worker_count = 10
    orch_port = find_free_port()
    # noinspection PyUnresolvedReferences
    hook = request.config.hook

    def process_reports():
        results = {}
        while True:
            report = report_q.get()
            report_q.task_done()
            if isinstance(report, TestReport):
                if report.when == 'call':
                    results.setdefault(report.outcome, 0)
                    results[report.outcome] += 1
                # hook.pytest_runtest_logreport(report=report)
                # print(f"Test results {report.nodeid}: {report.outcome}")
            elif isinstance(report, (ReportFinished, ReportStarted, WorkerExited)):
                pass
            elif isinstance(report, AllWorkersDone):
                break
            else:
                print(f">>>>>>>>>>>>>>>>>>>>>>> Unknown report type {report}")
        assert results.get('passed') == 200
        assert results.get('failed') == 200

    port = find_free_port()
    worker_agent = worker_agent_factory.create_worker_agent(port, authkey)
    my_ip = _get_my_ip()
    orchestrator = Orchestrator.as_server(address=(my_ip, orch_port), authkey=authkey)
    orchestrator.start_globals()
    report_q = orchestrator.conduct_session(session_id="Session1", args=['--cores', '2', '-s', '--loop', '200',
                                                                       f'--ignore={str(Path(__file__).parent)}'
                                                                         ],
                                            cwd=TEST_PROXY_DIR, )
    for index in range(worker_count):
        orchestrator.start_worker(session_id="Session1", worker_id=f"Worker-{index}", address=(_get_my_ip(), port),)
    process_reports()
    status = orchestrator.join_session(session_id="Session1", timeout=120).conjugate()
    always_print(">>>>>>>>>>>>>>>>>>>>>>>>> ORCHESTRATION COMPLETE <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                 as_error=True)
    worker_agent.shutdown()
    assert status == 0


# noinspection PyUnusedLocal
def test_session_with_localhost_only(request, shield):
    authkey = secrets.token_bytes(64)
    debug_print(f"Using auth key {authkey.hex()}")
    worker_count = 10
    ports = [find_free_port() for _ in range(worker_count)]
    # noinspection PyUnresolvedReferences
    report_q = JoinableQueue()
    hook = request.config.hook

    def process_reports():
        results = {}
        while True:
            report = report_q.get()
            report_q.task_done()
            if isinstance(report, TestReport):
                if report.when == 'call':
                    results.setdefault(report.outcome, 0)
                    results[report.outcome] += 1
                # pytest_runtest_logreport(report=report)
                # print(f"Test results {report.nodeid}: {report.outcome}")
            elif isinstance(report, (ReportFinished, ReportStarted)):
                pass
            elif isinstance(report, WorkerExited):
                pass
            elif isinstance(report, AllWorkersDone):
                break
            else:
                print(f">>>>>>>>>>>>>>>>>>>>>>> Unknown report type {report}")
        assert results.get('passed') == 200
        assert results.get('failed') == 200
    orchestrator = Orchestrator.as_local()
    orchestrator.start_globals()
    report_q = orchestrator.conduct_session(session_id="Session1", args=['--cores', '2', '-s', '--loop', '200'],
                                            cwd=TEST_PROXY_DIR)
    for port in ports:
        orchestrator.start_worker(session_id="Session1", worker_id=f"Worker-{port}", address=None)
    process_reports()
    status = orchestrator.join_session(session_id="Session1", timeout=120).conjugate()
    always_print(">>>>>>>>>>>>>>>>>>>>>>>>> ORCHESTRATION COMPLETE <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                 as_error=True)
    assert status == 0


@pytest.fixture(autouse=True)
def dir_cleanup():
    for dir_name in os.listdir("resources/project_tests"):
        if dir_name.startswith('Worker-'):
            shutil.rmtree(Path("resources") / 'project_tests' / dir_name)
