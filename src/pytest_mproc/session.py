"""
Top level definitions to start and stop sessions
"""
import asyncio
import json
import multiprocessing
import os
import sys
import uuid
from abc import abstractmethod, ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, AsyncIterator, Tuple, Optional

from _pytest.reports import TestReport

from pytest_mproc import AsyncMPQueue
from pytest_mproc.data import WorkerExited, ReportStarted, ReportFinished
from pytest_mproc.orchestration import Orchestrator
from pytest_mproc.user_output import debug_print


@dataclass
class RemoteWorkerNode:
    resource_id: str
    address: Tuple[str, int]
    addl_params: Dict[str, str]
    env: Dict[str, str]
    flags: List[str]

    @classmethod
    def from_json(cls, json_text: str):
        """
        :return RemoteNode instance from provided json string
           json format should be:
             {'resource_id': <id of resource, unique to session>,
              'host': <hostname of node>,
              'port': <port of node>,
              'addl_params': <dict based on str-key where any key in all caps will be treated as environment var to be
                 set for worker, and all others will be treated as command line potion to pytest (--<key> <value>)>
              'flags': name of flags to be added to pytest cmd line (will be translated to --<flag-name>, aka no need
                       for '--' in name)
              }
            DO NOT put '--' in the addl_params or flags names
        """
        json_data = json.loads(json_text)
        return RemoteWorkerNode(
            resource_id=json_data['worker_id'],
            address=(json_data['host'], int(json_data['port'])),
            addl_params={key: str(value) for key, value in json_data['addl_params'].items() if key.upper() != key},
            env={key: str(value) for key, value in json_data['addl_params'] if key.upper() == key},
            flags=json_data['flags']
        )


class ResourceManager(ABC):

    # noinspection PyUnusedLocal
    @abstractmethod
    async def reserve(self, count: int) -> AsyncIterator[RemoteWorkerNode]:
        """
        :param count: how many resources to reserve
        :return: an async iterator, yielding with RemoteNode as they become available.  For systems that require
          additional preparation time, there may be a gap in time between yield of each node based on this prep time
          NOTE: the worker id return in each RemoteWorkerNode must be unique to the session
        """
        raise NotImplementedError()
        # noinspection PyUnreachableCode
        yield None

    @abstractmethod
    async def relinquish(self, worker_id: str):
        """
        Relinquish the resource associated with a RemoteWorkerNode that has previously been reserved (via call to
           reserve() )
        :param worker_id: unique string id given for the RemoteWorkerNode upon reservation
        """


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
                 orchestrator_authkey: Optional[bytes] = None):
        if orchestrator_address is not None and orchestrator_authkey is None:
            raise RuntimeError("When supplying an address, must also supply an authkey")
        self._pending_reports: Dict[str, TestReport] = {}
        self._started_reports: Dict[str, ReportStarted] = {}
        self._authkey = orchestrator_authkey
        self._orchestrator_address = orchestrator_address
        self._is_local = orchestrator_address is None
        # noinspection PyTypeChecker
        self._orchestrator = Orchestrator.as_local() if self._is_local \
            else Orchestrator.as_client(self._orchestrator_address, orchestrator_authkey)
        self._mp = multiprocessing.Manager() if not self._is_local else None
        self._report_q: Optional[AsyncMPQueue] = None
        self._global_mgr_address: Optional[Tuple[str, int]] = None
        self._session_id = self._new_session_id()
        self._worker_nodes: Dict[str, Optional[RemoteWorkerNode]] = {}
        self._resource_mgr = resource_mgr

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    @property
    def session_id(self):
        return self._session_id

    async def start(self, worker_count: int, addl_args: List[str]):
        """
        Start a session with the given number of workers
        :param worker_count: how many workers to invoke
        :param addl_args: additional arguments to pass to pytest common to all workers (add'l to sys.argv)
        """
        args = sys.argv[1:] + addl_args
        host, port, authkey = self._orchestrator.start_globals()
        self._global_mgr_address = (host, port)
        report_q = self._orchestrator.start_session(session_id=self._session_id, args=args, cwd=Path(os.getcwd()))
        self._report_q = AsyncMPQueue(report_q)
        if self._resource_mgr is not None:
            async for node in self._resource_mgr.reserve(worker_count):
                if node.resource_id in self._worker_nodes:
                    raise ValueError(f"Worker id {node.resource_id} provided when reserving a resource already exists")
                self._worker_nodes[node.resource_id] = node
                self._orchestrator.start_worker(session_id=self._session_id, worker_id=f"Worker-{node.resource_id}",
                                                address=node.address)
        else:
            for index in range(worker_count):
                self._orchestrator.start_worker(session_id=self._session_id, worker_id=f"Worker-{index}",
                                                address=None)
                self._worker_nodes[str(index + 1)] = None
                print(f"!!!!!!!  {self._worker_nodes}")

    def shutdown(self):
        self._orchestrator.shutdown_session(session_id=self._session_id)
        if self._mp is not None:
            self._mp.shutdown()

    async def process_reports(self, hook):
        """
        Process incoming reports from report queue
        """
        # NOTE: most pytest runners, etc., assume that all start-log-finish of test (reports) happens serially
        # In parallel test execution, however, multiple tests can be started, be executing and finish at one time.
        # To avoid problems with assumptions of non-parallel execution, we multiplex and force serialization
        # of log start-report-finish operations
        tasks: List[asyncio.Task] = []

        while True:
            report = await self._report_q.get()
            if isinstance(report, WorkerExited):
                resource_id = report.worker_id[7:]  # remote "Worker-" prefix that was added
                if self._resource_mgr:
                    tasks.append(asyncio.create_task(self._resource_mgr.relinquish(resource_id)))
                del self._worker_nodes[resource_id]
                if not self._worker_nodes:
                    debug_print("All worker nodes have exited. No more reports to process")
                    break
            else:
                if isinstance(report, TestReport):
                    self._pending_reports[report.nodeid] = report
                elif isinstance(report, ReportStarted):
                    # We assume a smart strategy of ordering where longest-running tests are prioritized to run first
                    self._started_reports[report.nodeid] = report
                elif isinstance(report, ReportFinished):
                    if len(self._started_reports) == 0:
                        # should never really get here
                        hook.pytest_runtest_logfinish(nodeid=report.nodeid, location=report.location)
                        return
                    to_start = self._started_reports.get(report.nodeid)
                    if to_start is not None:
                        del self._started_reports[report.nodeid]
                        hook.pytest_runtest_logstart(nodeid=to_start.nodeid, location=to_start.location)
                    test_report = self._pending_reports.get(report.nodeid)
                    if test_report is not None:
                        del self._pending_reports[report.nodeid]
                        hook.pytest_runtest_logreport(test_report)
                    hook.pytest_runtest_logfinish(nodeid=report.nodeid, location=report.location)
        if tasks:
            await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
