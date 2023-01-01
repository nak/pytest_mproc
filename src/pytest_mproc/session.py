"""
Top level definitions to start and stop sessions
"""
import asyncio
import multiprocessing
import os
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from _pytest.reports import TestReport

from pytest_mproc import AsyncMPQueue
from pytest_mproc.data import WorkerExited, ReportStarted, ReportFinished, AllWorkersDone
from pytest_mproc.orchestration import Orchestrator
from pytest_mproc.resourcing import ResourceManager, RemoteWorkerNode
from pytest_mproc.user_output import debug_print, always_print
from threading import Semaphore


class Session:
    """
    Overarching Session for a test run.  This is the main overseer of execution, providing a thin interface to
    the external client while keeping the details of orchestrator/worker execution hidden as internals
    """

    @classmethod
    def _new_session_id(cls) -> str:
        new_id = uuid.uuid4().hex
        return new_id

    # noinspection PyUnresolvedReferences
    def __init__(self, resource_mgr: Optional[ResourceManager] = None,
                 orchestrator_address: Optional[Tuple[str, int]] = None,
                 authkey: Optional[bytes] = None):
        if orchestrator_address is not None and authkey is None:
            raise RuntimeError("When supplying an address, must also supply an authkey")
        self._pending_reports: Dict[str, List[TestReport]] = {}
        self._started_reports: Dict[str, ReportStarted] = {}
        self._authkey = authkey
        self._orchestrator_address = orchestrator_address
        self._is_local = orchestrator_address is None
        # noinspection PyTypeChecker
        self._orchestrator = Orchestrator.as_local(has_remote_workers=resource_mgr is not None) if self._is_local \
            else Orchestrator.as_client(self._orchestrator_address, authkey, resource_mgr is not None)
        self._report_q: Optional[AsyncMPQueue] = None
        self._global_mgr_address: Optional[Tuple[str, int]] = None
        self._session_id = self._new_session_id()
        self._worker_nodes: Dict[str, Optional[RemoteWorkerNode]] = {}
        self._resource_mgr = resource_mgr
        self._sem = Semaphore(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            always_print(f"Exception in cleanup of session: {exc_type}:{exc_val}")
        self.shutdown(on_error=exc_type is not None)

    @property
    def session_id(self):
        return self._session_id

    async def start(self, worker_count: int, pytest_args: List[str]):
        """
        Start a session with the given number of workers
        :param worker_count: how many workers to invoke
        :param pytest_args: additional arguments to pass to pytest common to all workers (add'l to sys.argv)
        """
        if self._resource_mgr is not None:
            host, port, authkey = self._orchestrator.start_globals()
            self._global_mgr_address = (host, port)
        else:
            self._global_mgr_address = None
        report_q = self._orchestrator.start_session(
            session_id=self._session_id, args=pytest_args, cwd=Path(os.getcwd())
        )
        self._report_q = AsyncMPQueue(report_q)
        self._sem.release()
        if self._resource_mgr is not None:
            async for node in self._resource_mgr.reserve(worker_count):
                if node.resource_id in self._worker_nodes:
                    raise ValueError(f"Worker id {node.resource_id} provided when reserving a resource already exists")
                self._worker_nodes[node.resource_id] = node
                self._orchestrator.start_worker(session_id=self._session_id, worker_id=f"Worker-{node.resource_id}",
                                                address=node.address, agent_authkey=node.authkey)
        else:
            for index in range(worker_count):
                self._orchestrator.start_worker(session_id=self._session_id, worker_id=f"Worker-{index + 1}",
                                                address=None)
                self._worker_nodes[str(index + 1)] = None
        self._orchestrator.workers_exhausted(self._session_id)

    def wait_on_start(self):
        self._sem.acquire()

    def shutdown(self, on_error: bool = False) -> int:
        status = self._orchestrator.shutdown_session(session_id=self._session_id, on_error=on_error)
        return status if not on_error else -1

    async def process_reports(self, hook):
        """
        Process incoming reports from report queue
        """
        # NOTE: most pytest runners, etc., assume that all start-log-finish of test (reports) happens serially
        # In parallel test execution, however, multiple tests can be started, be executing and finish at one time.
        # To avoid problems with assumptions of non-parallel execution, we multiplex and force serialization
        # of log start-report-finish operations
        tasks: List[asyncio.Task] = []

        def process(report_) -> bool:
            if isinstance(report_, WorkerExited):
                resource_id = report_.worker_id[7:]  # remote "Worker-" prefix that was added
                if self._resource_mgr:
                    tasks.append(asyncio.create_task(self._resource_mgr.relinquish(resource_id)))
                del self._worker_nodes[resource_id]
                return len(self._worker_nodes) == 0
            elif isinstance(report_, AllWorkersDone):
                debug_print(f"No more workers active and no reports to process")
                return True
            else:
                if isinstance(report_, TestReport):
                    self._pending_reports.setdefault(report_.nodeid, []).append(report_)
                elif isinstance(report_, ReportStarted):
                    # We assume a smart strategy of ordering: longest-running tests are prioritized to run first
                    self._started_reports[report_.nodeid] = report_
                elif isinstance(report_, ReportFinished):
                    if len(self._started_reports) == 0:
                        # should never really get here
                        hook.pytest_runtest_logfinish(nodeid=report_.nodeid, location=report_.location)
                        return False
                    to_start = self._started_reports.get(report_.nodeid)
                    if to_start is not None:
                        del self._started_reports[report_.nodeid]
                        hook.pytest_runtest_logstart(nodeid=to_start.nodeid, location=to_start.location)
                    test_reports = self._pending_reports.get(report_.nodeid)
                    for test_report in test_reports:
                        hook.pytest_runtest_logreport(report=test_report)
                    del self._pending_reports[report_.nodeid]
                    hook.pytest_runtest_logfinish(nodeid=report_.nodeid, location=report_.location)
            return False
        try:
            always_print(f">>>>>>>>>>>>>>>>> GETTING")
            report = await self._report_q.get()
            always_print(f">>>>>>>>>>>>>>>>> GOT {report}")
            while report is not None:
                process(report)
                always_print(f">>>>>>>>>>>>>>>>> GETTING")
                report = await self._report_q.get()
                always_print(f">>>>>>>>>>>>>>>>> GOT {report}")
            debug_print(f"No more reports to process {self._pending_reports}")
            if tasks:
                await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        except Exception as e:
            always_print(f"Exception processing reports: {e} [{type(e)}]. Shutting down")
            self.shutdown(on_error=True)
        finally:
            with suppress(Exception):
                self._report_q.raw().close()
