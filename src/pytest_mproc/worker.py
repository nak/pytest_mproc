import multiprocessing
import os
import signal
from dataclasses import dataclass
from multiprocessing.managers import BaseManager

import pytest
import resource
import sys
import time
import traceback

from contextlib import suppress
from multiprocessing import (
    JoinableQueue,
    Queue,
)
from typing import (
    Iterator,
    List,
    Union, Optional, Tuple, Dict,
)

from pytest_mproc.fixtures import Global, Node
from pytest_mproc.user_output import always_print
from pytest_mproc import _get_my_ip, _find_free_port
from pytest_mproc.exceptions import TestError
from pytest_mproc.data import (
    ClientExited,
    ReportFinished,
    ReportStarted,
    ResourceUtilization,
    ResultException,
    ResultExit,
    ResultTestStatus,
    ResultType,
    TestBatch,
    TestState,
    TestStateEnum, resource_utilization,
)
from pytest_mproc.utils import BasicReporter


"""maximum time between reporting status back to coordinator"""
MAX_REPORTING_INTERVAL = 1.0  # seconds


class WorkerSession:
    """
    Handles reporting of test status and the like
    """

    def __init__(self, worker_id: str, test_q: JoinableQueue, results_q: Queue, results_buffer_size: int = 5):
        self._this_host = _get_my_ip()
        self._id = worker_id
        self._name = worker_id
        self._count = 0
        self._session_start_time = time.time()
        self._buffered_results = []
        self._buffer_size = results_buffer_size
        self._timestamp = time.time()
        self._last_execution_time = time.time()
        self._resource_utilization = ResourceUtilization(-1.0, -1.0, -1.0, -1)
        self._reporter = BasicReporter()
        self._test_q = test_q
        self._result_q = results_q
        self._ip_addr = _get_my_ip()
        self._pid = os.getpid()
        self._current = None
        self._test_generator = None
        self._named_items = {}

    def _put(self, result: Union[TestStateEnum, ResultType, ResultExit, ClientExited,
                                 ReportStarted, ReportFinished], timeout=None):
        """
        Append test result data to queue, flushing buffered results to queue at watermark level for efficiency
        :param result: test result data
        :param timeout: timeout after this many seconds, if specified
        :raises: TimeoutError is not executed in time
        """
        self._buffered_results.append(result)
        if isinstance(result, (ClientExited, TestStateEnum, ResultExit)) \
                or len(self._buffered_results) >= self._buffer_size \
                or (time.time() - self._timestamp) > MAX_REPORTING_INTERVAL:
            self._flush(timeout)

    def _flush(self, timeout=None):
        """
        flush buffered results out to the queue.
        """
        if self._buffered_results:
            if timeout is not None:
                self._result_q.put(self._buffered_results, timeout)
            else:
                self._result_q.put(self._buffered_results)
            self._buffered_results = []
            self._timestamp = time.time()

    def test_loop(self, session):
        """
        This is where the action takes place.  We override the usual implementation since
        that doesn't support a dynamic generation of tests (whose source is the test Queue
        that it draws from to pick out the next test)
        :param session:  Where the test generator is kept
        """
        # global my_cov
        start_time = time.time()
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        try:
            if session.testsfailed and not session.config.option.continue_on_collection_errors:
                raise session.Failed("%d errors during collection" % session.testsfailed)

            if hasattr(session.config, 'option') and session.config.option.collectonly:
                return  # should never really get here, but for consistency
            for test_batch in self._test_generator:
                if test_batch is None:
                    break  # sentinel received
                # test item comes through as a unique string nodeid of the test
                # We use the pytest-collected mapping we squirrelled away to look up the
                # actual _pytest.python.Function test callable
                # NOTE: session.items is an iterator, as set upon construction of WorkerSession
                for test_id in test_batch.test_ids:
                    bare_test_id = test_id.split(os.sep)[-1]
                    item = self._named_items[bare_test_id]
                    self._put(TestState(TestStateEnum.STARTED, self._this_host, self._pid, test_id, test_batch))
                    self._current = (test_id, test_batch)
                    try:
                        # run the test
                        item.config.hook.pytest_runtest_protocol(item=item, nextitem=None)
                    finally:
                        self._put(TestState(TestStateEnum.FINISHED, self._this_host, self._pid,
                                            test_id, test_batch))
                    # very much like that in _pytest.main:
                    try:
                        if session.shouldfail:
                            raise session.Failed(session.shouldfail)
                    except AttributeError:
                        pass  # some version of pytest do not have this attribute
                    if session.shouldstop:
                        raise session.Interrupted(session.shouldstop)
                    # count tests that have been run
                    self._count += 1
                    self._last_execution_time = time.time()
        except (EOFError, ConnectionError, BrokenPipeError, KeyboardInterrupt) as e:
            always_print(f"{self._name} terminating, testing queue closed unexpectedly")
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:
            with suppress(Exception):
                self._flush()
            with suppress(Exception):
                end_usage = resource.getrusage(resource.RUSAGE_SELF)
                time_span = self._last_execution_time - start_time
                self._resource_utilization = resource_utilization(time_span=time_span,
                                                                  start_rusage=rusage,
                                                                  end_rusage=end_usage)
            with suppress(Exception):
                self.session_finish()

    # noinspection PyUnusedLocal
    def session_finish(self):
        """
        output failure information and final exit status back to coordinator
        """
        self._put(ResultExit(self._name,
                             self._count,
                             0,
                             self._last_execution_time - self._session_start_time,
                             self._resource_utilization))
        with suppress(Exception):
            self._flush()
        return True

    def pytest_collection_finish(self, session):
        """
        Invoked once pytest has collected all information about which tests to run.
        Those items are squirrelled away in a different attributes, and a generator
        is put in its place to draw from.
        :param session: the pytest session
        :return: the generator of tests to run
        """
        def generator() -> Iterator[TestBatch]:
            try:
                test = self._test_q.get()
                while test:
                    self._test_q.task_done()
                    yield test
                    test = self._test_q.get()
                self._test_q.task_done()
            except (EOFError, BrokenPipeError, ConnectionError, KeyboardInterrupt) as e:
                always_print(f"{self._name} terminating, testing queue closed unexpectedly")
                if isinstance(e, KeyboardInterrupt):
                    raise
        self._test_generator = generator()
        self._named_items = {item.nodeid.split(os.sep)[-1]: item for item in session.items}
        return session.items

    #####################
    #  Pytest hook called from plugin.py, but specific to this worker session:

    # noinspection SpellCheckingInspection
    def pytest_internalerror(self, __excrepr):
        try:
            self._put(ResultException(SystemError("Internal pytest error")))
        except Exception as e:
            always_print(f"{self._name} failed to pust internal error: {e}")

    # noinspection SpellCheckingInspection
    def pytest_runtest_logstart(self, nodeid, location):
        try:
            self._put(ReportStarted(nodeid, location))
        except Exception as e:
            always_print(f"{self._name} failed to post start of report: {e}")

    # noinspection SpellCheckingInspection
    def pytest_runtest_logfinish(self, nodeid, location):
        try:
            self._put(ReportFinished(nodeid, location))
        except Exception as e:
            always_print(f"{self._name} failed to post finish of report: {e}")

    # noinspection SpellCheckingInspection
    def pytest_runtest_logreport(self, report):
        """
        report only status of calls (not setup or teardown), unless there was an error in those
        stash report for later end-game parsing of failures

        :param report: report to draw info from
        """
        with suppress(Exception):
            if hasattr(report.longexpr, "type") and issubclass(report.longexpr.type, TestError) and self._current:
                te: TestError = report.longexpr.value
                test_id, test_batch = self._current
                if te.retry:
                    self._put(TestState(TestStateEnum.RETRY, self._this_host, self._pid, test_id, test_batch))
                else:
                    self._put(ResultTestStatus(report))
                    self._put(TestState(TestStateEnum.FINISHED, self._this_host, self._pid, test_id, test_batch))
                if te.fatal:
                    pytest.exit("Test system fault detected.  Aborting worker test exeuction")
                return
        try:
            self._put(ResultTestStatus(report))
        except Exception as e:
            always_print(f"Failed to log report to main node: {e}")

    # End pytest hooks
    ##########################

    @classmethod
    def set_singleton(cls, singleton):
        cls._singleton = singleton

    @classmethod
    def singleton(cls):
        return cls._singleton

    _singleton = None


class WorkerAgent(BaseManager):
    _sessions: Dict[str, "WorkerAgent.TestSession"] = {}
    _node_mgrs: Dict[str, Node.Manager] = {}

    @dataclass
    class TestSession:
        test_q: JoinableQueue
        results_q: JoinableQueue
        global_mgr_address: Optional[Tuple[str, int]]
        node_mgr_port: Optional[int]
        pytest_args: List[str]
        worker_procs: Dict[str, multiprocessing.Process]
        authkey: bytes
        active: bool = True

    def __int__(self, address: Optional[Tuple[str, int]] = None, authkey: Optional[bytes] = None):
        if address is not None:
            if authkey is None:
                raise RuntimeError(f"Must supply authkey when address is not None")
            super().__init__(address, authkey=authkey)

    @classmethod
    def as_server(cls, address: Tuple[str, int], authkey: bytes) -> "WorkerAgent":
        cls.register("start_session", cls.start_session)
        cls.register("start_worker", cls.start_worker)
        agent = WorkerAgent(address=address, authkey=authkey)
        agent.start()
        return agent

    @classmethod
    def as_client(cls, address: Tuple[str, int], authkey: bytes) -> "WorkerAgent":
        cls.register("start_session")
        cls.register("start_worker")
        agent = WorkerAgent(address=address, authkey=authkey)
        agent.connect()
        return agent

    @classmethod
    def as_local(cls):
        return WorkerAgent(address=None, authkey=None)

    @classmethod
    def start_session(cls, session_id: str, test_q: JoinableQueue, results_q: JoinableQueue, args: List[str],
                      authkey: bytes,
                      global_mgr_address: Optional[Tuple[str, int]] = None):
        if session_id in cls._sessions:
            raise KeyError(f"Worker TestSession with is {session_id} already exists")
        node_mgr_port = _find_free_port()
        cls._node_mgrs[session_id] = Node.Manager.singleton(node_mgr_port, authkey)
        cls._sessions[session_id] = cls.TestSession(test_q, results_q, global_mgr_address, node_mgr_port, args, {},
                                                    authkey=authkey)

    @classmethod
    def start_worker(cls, session_id: str, worker_id: str):
        """
        Start a worker within a given test session
        :param session_id: which session
        :param worker_id: unique id associated with worker
        """
        if session_id not in cls._sessions:
            raise KeyError(f"No such session: {session_id}")
        test_session = cls._sessions[session_id]
        if not test_session.active:
            raise RuntimeError("Attempt to start worker when test session is not actively accepting new workers")
        proc = multiprocessing.Process(target=main, args=(test_session.test_q,
                                                          test_session.results_q,
                                                          test_session.pytest_args,
                                                          worker_id,
                                                          test_session.authkey,
                                                          test_session.global_mgr_address,
                                                          test_session.node_mgr_port))

        test_session.worker_procs[worker_id] = proc
        proc.start()

    def shutdown_session(self, session_id: str, timeout: Optional[float] = None):
        """
        shutdown a session by joining with all worker processes
        :param session_id: which session
        :param timeout: optional timeout value for waiting on a worker, after which the worker will be terminated
            explicitly
        """
        test_session = self._sessions.get(session_id)
        node_mgr = self._node_mgrs.get(session_id)
        if node_mgr:
            node_mgr.shutdown()
        if test_session:
            for worker_id, worker in test_session.worker_procs.items():
                worker.join(timeout)
                if worker.exitcode is None:
                    worker.terminate()
                    always_print(f"Terminated worker {worker_id} abruptly", as_error=True)


def main(test_q: JoinableQueue, results_q: JoinableQueue, args: List[str], worker_id: str,
         authkey: bytes,
         global_mgr_address: Optional[Tuple[str, int]] = None, node_mgr_port: Optional[int] = None):
    from pytest_mproc import plugin  # ensures auth_key is set
    assert plugin  # to prevent flake8 unused import
    if global_mgr_address:
        Global.Manager.as_client(global_mgr_address, auth_key=authkey)
    if node_mgr_port:
        Node.Manager.singleton(node_mgr_port, authkey=authkey)
    # noinspection PyUnresolvedReferences
    from pytest_mproc.worker import WorkerSession  # to make Python happy
    worker = WorkerSession(worker_id=worker_id,
                           test_q=test_q,
                           results_q=results_q,
                           )
    WorkerSession.set_singleton(worker)
    assert WorkerSession.singleton() is not None
    status = None
    # noinspection PyBroadException
    try:
        status = pytest.main(args)
        if isinstance(status, pytest.ExitCode):
            status = status.value
    except Exception as e:
        msg = f"Worker {_get_my_ip()}-{os.getpid()} died with exception {e}\n {traceback.format_exc()}"
        os.write(sys.stderr.fileno(), msg.encode('utf-8'))
        with suppress(Exception):  # if other end died, result_q is gone, so suppress exceptions
            results_q.put(ClientExited(os.getpid(), _get_my_ip(), errored=True, message=msg))
    finally:
        signal.alarm(10)
        results_q.join()

    sys.exit(status or -1)
