import asyncio
import multiprocessing
import os
import secrets
import threading
import time
from multiprocessing import JoinableQueue
from pathlib import Path
from typing import List, AsyncIterator, Optional, Tuple
from unittest.mock import patch

import pytest
from _pytest.reports import TestReport

from pytest_mproc import find_free_port
from pytest_mproc.data import ReportStarted, ReportFinished, WorkerExited
from pytest_mproc.mp import SharedJoinableQueue
from pytest_mproc.orchestration import Orchestrator
from pytest_mproc.session import Session
from pytest_mproc.resourcing import ResourceManager, RemoteWorkerNode


class MockHook:
    started = []
    finished = []
    reported = []

    def pytest_runtest_logreport(self, report: TestReport):
        assert report.outcome == 'passed'
        self.reported.append(report.nodeid)

    def pytest_runtest_logstart(self, nodeid: str, location):
        print(f">>>>>>>>>>>>>>>>>>>>>>> STARTED {nodeid}")
        self.started.append(nodeid)

    def pytest_runtest_logfinish(self, nodeid: str, location):
        self.finished.append(nodeid)


@pytest.fixture
def hook():
    yield MockHook()


@pytest.mark.asyncio
async def test_process_reports(hook):
    with Session() as session:

        def mock_launch(session_id: str, authkey: bytes, main_address: Optional[Tuple[str, int]],
                        main_authkey: Optional[bytes],
                        test_q: JoinableQueue, status_q: JoinableQueue,
                        report_q: JoinableQueue,
                        worker_q: JoinableQueue, args: List[str], cwd: Path, node_mgr_port: int):
            worker = worker_q.get()
            while worker is not None:
                time.sleep(1)
                worker = worker_q.get()

        with patch("pytest_mproc.orchestration.MainSession.launch", new=mock_launch):
            await session.start(worker_count=1, pytest_args=[])

            def populate():

                async def do():
                    items = []
                    for base in range(33):
                        for nodeid in [f'node-{index + 3*base}' for index in range(3)]:
                            items.append(ReportStarted(nodeid=nodeid, location="here"))
                        for nodeid in [f'node-{index + 3*base}' for index in range(3)]:
                            items.append(TestReport(nodeid=nodeid, location=("there", None, ""), longrepr="", when='call', keywords={},
                                                    outcome='passed'))
                        for nodeid in [f'node-{index+ 3*base}' for index in range(3)]:
                            items.append(ReportFinished(nodeid=nodeid, location="done"))
                    for item in items:
                        await session._report_q.put(item)
                    await session._report_q.put(WorkerExited(worker_id="Worker-1", pid=os.getpid(), host='localhost'))
                    await session._report_q.put(None)
                asyncio.run(do())

            proc = multiprocessing.Process(target=populate)
            proc.start()

            await session.process_reports(hook)
            assert hook.started == [f'node-{i}' for i in range(99)]
            assert hook.finished == [f'node-{i}' for i in range(99)]
            assert hook.reported == [f'node-{i}' for i in range(99)]


@pytest.mark.asyncio
async def test_process_reports_nonlocal(hook):
    ports = [find_free_port() for _ in range(14)]

    class ResourceMgr(ResourceManager):

        async def relinquish(self, worker_id: str):
            pass

        async def reserve(self, count: int) -> AsyncIterator[RemoteWorkerNode]:
            for i in range(count):
                yield RemoteWorkerNode(resource_id=f'resource_{i}',
                                       address=('localhost', ports[i]),
                                       addl_params={'device': f"DEVICE_ID{i}"},
                                       env={}, flags=['-s'],
                                       authkey=b'')

    port = find_free_port()

    def mock_launch(session_id: str, authkey: bytes, main_address: Optional[Tuple[str, int]],
                        main_authkey: Optional[bytes],
                        test_q: JoinableQueue, status_q: JoinableQueue,
                        report_q: JoinableQueue,
                        worker_q: JoinableQueue, args: List[str], cwd: Path, node_mgr_port: int):
        worker = worker_q.get()
        while worker is not None:
            start_worker(report_q, session_id, worker[0], None)
            time.sleep(0.2)
            worker = worker_q.get()

    nodeids = {f'Worker-resource_{index}': [
        f'node-{j + index * 8}' for j in range(8)
    ] for index in range(14)}

    def start_worker(report_q, session_id: str, worker_id: str, address):
        assert worker_id in [f'Worker-resource_{i}' for i in range(14)]

        def worker():
            nonlocal nodeids
            for nodeid in nodeids[worker_id]:
                print(f">>>>>>>>>>>>> PUTTING {nodeid}")
                report_q.put(ReportStarted(nodeid=nodeid, location="here"))
                report_q.put(TestReport(nodeid=nodeid, location=("there", None, ""),
                                                    longrepr="", when='call', keywords={},
                                                    outcome='passed'))
                report_q.put(ReportFinished(nodeid=nodeid, location="here"))
            report_q.put(WorkerExited(worker_id=worker_id,
                                            pid=os.getpid(), host='localhost'))
            report_q.put(None)
        thread = threading.Thread(target=worker)
        thread.start()

    authkey = secrets.token_bytes(64)
    resource_mgr = ResourceMgr()

    def start_serve():
        server = Orchestrator.as_server(address=('localhost', port), authkey=authkey)
        time.sleep(1000)

    with patch("pytest_mproc.orchestration.MainSession.launch", new=mock_launch):
        proc = multiprocessing.Process(target=start_serve)
        proc.start()
        # orchestrator = Orchestrator.as_server(address=('localhost', port), authkey=authkey)
        time.sleep(3)
        try:
            with Session(resource_mgr=resource_mgr, orchestrator_address=('localhost', port),
                         authkey=authkey) as session:
                await session.start(worker_count=14, pytest_args=[])
                await session.process_reports(hook)
                assert set(hook.started) == {f'node-{i}' for i in range(112)}, "Not all tests started"
                assert set(hook.finished) == {f'node-{i}' for i in range(112)}, "Not all tests finished"
                assert set(hook.reported) == {f'node-{i}' for i in range(112)}, "Not all tests reported"

        finally:
            # orchestrator.shutdown()
            proc.kill()