import binascii
import multiprocessing
import os
import signal
from dataclasses import dataclass
from multiprocessing.managers import SyncManager
from pathlib import Path

import pytest
import resource
import sys
import time
import traceback

from contextlib import suppress
from multiprocessing import (
    JoinableQueue,
)
from typing import (
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
)

from pytest_mproc.fixtures import Global, Node
from pytest_mproc.mp import SafeSerializable
from pytest_mproc.user_output import always_print, debug_print
from pytest_mproc import _get_my_ip, _find_free_port
from pytest_mproc.exceptions import TestError
from pytest_mproc.data import (
    WorkerExited,
    ReportFinished,
    ReportStarted,
    ResourceUtilization,
    StatusTestsComplete,
    StatusType,
    TestBatch,
    StatusTestState,
    TestStateEnum, resource_utilization, ClientException, ReportUpdate, WorkerStarted,
)
from pytest_mproc.utils import BasicReporter


"""maximum time between reporting status back to coordinator"""
MAX_REPORTING_INTERVAL = 1.0  # seconds


class WorkerSession:
    """
    Handles reporting of test status and the like
    """

    def __init__(self, session_id: str, worker_id: str, test_q: JoinableQueue, status_q: JoinableQueue,
                 report_q: JoinableQueue,
                 results_buffer_size: int = 5):
        self._this_host = _get_my_ip()
        self._id = worker_id
        self._name = worker_id
        self._session_id = session_id
        self._count = 0
        self._session_start_time = time.time()
        self._buffered_results = []
        self._buffer_size = results_buffer_size
        self._timestamp = time.time()
        self._last_execution_time = time.time()
        self._resource_utilization = ResourceUtilization(-1.0, -1.0, -1.0, -1)
        self._reporter = BasicReporter()
        self._test_q = test_q
        self._status_q = status_q
        self._report_q = report_q
        self._ip_addr = _get_my_ip()
        self._pid = os.getpid()
        self._current = None
        self._test_generator = None
        self._named_items = {}

    def _put(self, status: StatusType, timeout=None):
        """
        Append test result data to queue, flushing buffered results to queue at watermark level for efficiency
        :param status: test result data
        :param timeout: timeout after this many seconds, if specified
        :raises: TimeoutError is not executed in time
        """
        self._buffered_results.append(status)
        if isinstance(status, (WorkerExited, TestStateEnum, StatusTestsComplete)) \
                or len(self._buffered_results) >= self._buffer_size \
                or (time.time() - self._timestamp) > MAX_REPORTING_INTERVAL:
            self._flush(timeout)

    def _report(self, report_item: ReportUpdate):
        self._report_q.put(report_item)

    def _flush(self, timeout=None):
        """
        flush buffered results out to the queue.
        """
        if self._buffered_results:
            if timeout is not None:
                self._status_q.put(self._buffered_results, timeout)
            else:
                self._status_q.put(self._buffered_results)
            self._buffered_results = []
            self._timestamp = time.time()

    def test_loop(self, session):
        """
        This is where the action takes place.  We override the usual implementation since
        that doesn't support a dynamic generation of tests (whose source is the test Queue
        that it draws from to pick out the next test)
        :param session:  Where the test generator is kept
        """
        debug_print(f"Worker {self._id} started test loop...")
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
                    self._put(StatusTestState(TestStateEnum.STARTED, self._this_host, self._pid, test_id, test_batch))
                    self._current = (test_id, test_batch)
                    try:
                        # run the test
                        item.config.hook.pytest_runtest_protocol(item=item, nextitem=None)
                    finally:
                        self._put(StatusTestState(TestStateEnum.FINISHED, self._this_host, self._pid,
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
        except KeyboardInterrupt:
            raise
        except (EOFError, ConnectionError, BrokenPipeError) as e:
            always_print(f"{self._name} terminating, test queue closed unexpectedly")
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
        self._put(StatusTestsComplete(session_id=self._session_id,
                                      worker_id=self._name,
                                      test_count=self._count,
                                      status=0,
                                      duration=self._last_execution_time - self._session_start_time,
                                      resource_utilization=self._resource_utilization))
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
            self._put(SystemError("Internal pytest error"))
        except Exception as e:
            always_print(f"{self._name} failed to pust internal error: {e}")

    # noinspection SpellCheckingInspection
    def pytest_runtest_logstart(self, nodeid, location):
        try:
            self._report(ReportStarted(nodeid, location))
        except Exception as e:
            always_print(f"{self._name} failed to post start of report: {e}")

    # noinspection SpellCheckingInspection
    def pytest_runtest_logfinish(self, nodeid, location):
        try:
            self._report(ReportFinished(nodeid, location))
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
                    self._put(StatusTestState(TestStateEnum.RETRY, self._this_host, self._pid, test_id, test_batch))
                else:
                    self._report(report)
                    self._put(StatusTestState(TestStateEnum.FINISHED, self._this_host, self._pid, test_id, test_batch))
                if te.fatal:
                    pytest.exit("Test system fault detected.  Aborting worker test exeuction")
                return
        try:
            self._report(report)
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


class WorkerAgent(SafeSerializable):
    _mp_server: Optional[SyncManager] = None
    _node_mgrs: Dict[str, Node.Manager] = {}
    _registered: bool = False
    _singleton: Dict[Tuple[str, int], "WorkerAgent"] = {}
    _proxies: Dict[Tuple[str, int], "WorkerAgent"] = {}

    @dataclass
    class TestSession:
        test_q: JoinableQueue
        status_q: JoinableQueue
        report_q: JoinableQueue
        global_mgr_address: Optional[Tuple[str, int]]
        node_mgr_port: Optional[int]
        pytest_args: List[str]
        worker_procs: Dict[str, multiprocessing.Process]
        authkey: bytes
        cwd: Path
        active: bool = True

    def __init__(self, address: Optional[Tuple[str, int]] = None):
        if address in self.__class__._singleton:
            raise RuntimeError("Attempt to instantiate worker agent twice on same host")
        self._node_mgr: Dict[str, Node.Manager] = {}
        self._sessions: Dict[str, "WorkerAgent.TestSession"] = {}
        self._port = address[1] if address else None
        self._address = address
        self.__class__._singleton[address] = self

    @property
    def address(self):
        return self._address

    @classmethod
    def start_server(cls, address: Tuple[str, int], authkey: bytes) -> SyncManager:
        debug_print(f"worker agent as server: {address} {cls._mp_server} ")
        assert cls._mp_server is None
        SyncManager.register('start_session', cls._cls_start_session)
        SyncManager.register('shutdown_session', cls._cls_shutdown_session)
        SyncManager.register('start_worker', cls._cls_start_worker)
        SyncManager.register('ping', cls._cls_ping)
        SyncManager.register('shutdown_agent', cls.shutdown_agent)
        SyncManager.register('create', cls.create)  # only for server
        _mp_server = SyncManager(address, authkey)
        _mp_server.start()
        # noinspection PyUnresolvedReferences
        _mp_server.create(address)
        debug_print(f"Started worker agent server at {address}  {authkey.hex()}")
        # noinspection PyUnresolvedReferences
        return _mp_server

    @classmethod
    def create(cls, address):
        cls._singleton[address] = WorkerAgent(address)

    @classmethod
    def as_client(cls, address: Tuple[str, int], authkey: bytes) -> "WorkerAgent":
        debug_print(f"Worker agent client at {address}")
        if address in cls._proxies:
            return cls._proxies[address]
        if not cls._registered:
            SyncManager.register('start_session',)
            SyncManager.register('shutdown_session')
            SyncManager.register('start_worker')
            SyncManager.register('ping')
            SyncManager.register('shutdown_agent',)
            cls._registered = True
        debug_print(f"Connecting worker client at {address}... {authkey.hex()}")
        mp_client = SyncManager(address=address, authkey=authkey)
        mp_client.connect()
        debug_print(f"Connected worker agent client {authkey.hex()}")

        class Proxy:
            # noinspection PyMethodMayBeStatic
            def start_session(self, session_id: str,
                              main_address: Tuple[str, int], main_token: Optional[str],
                              test_q: JoinableQueue, status_q: JoinableQueue,
                              report_q: JoinableQueue,
                              cwd: Path, args: List[str], token: Optional[str] = None,
                              global_mgr_address: Optional[Tuple[str, int]] = None,
                              node_mgr_port: Optional[int] = None):
                try:
                    # noinspection PyUnresolvedReferences
                    return mp_client.start_session(
                        address=address,
                        session_id=session_id,
                        main_address=main_address,
                        main_token=main_token,
                        test_q=test_q, status_q=status_q,
                        report_q=report_q, cwd=cwd, args=args, token=token,
                        global_mgr_address=global_mgr_address,
                        node_mgr_port=node_mgr_port
                    )
                finally:
                    always_print("DONE PROXY START SESSION")

            # noinspection PyMethodMayBeStatic
            def start_worker(self, session_id: str, worker_id: str):
                # noinspection PyUnresolvedReferences
                return mp_client.start_worker(address=address, session_id=session_id, worker_id=worker_id)

            # noinspection PyMethodMayBeStatic
            def ping(self) -> str:
                # noinspection PyUnresolvedReferences
                return mp_client.ping(address)

            # noinspection PyMethodMayBeStatic
            def shutdown_session(self, session_id: str, timeout: Optional[float] = None):
                # noinspection PyUnresolvedReferences
                return mp_client.shutdown_session(address, session_id, timeout)

        # noinspection PyTypeChecker
        cls._proxies[address] = Proxy()
        return cls._proxies[address]

    @classmethod
    def as_local(cls):
        return WorkerAgent(address=None)

    @classmethod
    def _cls_start_session(cls, address: Tuple[str, int],
                           session_id: str,
                           main_address: Tuple[str, int],
                           main_token: Optional[str],
                           test_q: JoinableQueue, status_q: JoinableQueue, report_q: JoinableQueue,
                           cwd: Path, args: List[str], token: Optional[str] = None,
                           global_mgr_address: Optional[Tuple[str, int]] = None,
                           node_mgr_port: Optional[int] = None):
        return cls._singleton[address].start_session(
            session_id=session_id, main_address=main_address, main_token=main_token,
            test_q=test_q, status_q=status_q, report_q=report_q,
            cwd=cwd, args=args, token=token, global_mgr_address=global_mgr_address,
            node_mgr_port=node_mgr_port)

    # noinspection PyUnresolvedReferences
    def start_session(self, session_id: str,
                      main_address: Tuple[str, int], main_token: Optional[str],
                      test_q: JoinableQueue, status_q: JoinableQueue, report_q: JoinableQueue,
                      cwd: Path, args: List[str], token: Optional[str] = None,
                      global_mgr_address: Optional[Tuple[str, int]] = None,
                      node_mgr_port: Optional[int] = None):
        if session_id in self._sessions:
            raise KeyError(f"Worker TestSession '{session_id}' already exists")
        if test_q is None:
            sm = SyncManager(address=main_address, authkey=binascii.a2b_hex(main_token))
            sm.register('get_test_q')
            sm.register('get_status_q')
            sm.register('get_report_q')
            sm.connect()
            test_q = sm.get_test_q()
            status_q = sm.get_status_q()
            report_q = sm.get_report_q()
        authkey = binascii.a2b_hex(token) if token is not None else None
        self._sessions[session_id] =\
            self.__class__.TestSession(test_q, status_q, report_q, global_mgr_address, node_mgr_port,
                                       args, {}, cwd=cwd, authkey=authkey)
        debug_print(f"Started worker-session under {session_id} port {node_mgr_port}")

    @classmethod
    def _cls_start_worker(cls, address: Tuple[str, int], session_id: str, worker_id: str):
        return cls._singleton[address].start_worker(session_id=session_id, worker_id=worker_id)

    def start_worker(self, session_id: str, worker_id: str):
        """
        Start a worker within a given test session
        :param session_id: which session
        :param worker_id: unique id associated with worker
        """
        test_session = self._sessions[session_id]
        if worker_id in test_session.worker_procs:
            raise ValueError(f"Worker {worker_id} for session {session_id} already exists")
        if not test_session.active:
            raise RuntimeError("Attempt to start worker when test session is not actively accepting new workers")
        debug_print("Launching worker  process..")
        proc = multiprocessing.Process(target=main, args=(test_session.test_q,
                                                          test_session.status_q,
                                                          test_session.report_q,
                                                          test_session.cwd,
                                                          test_session.pytest_args,
                                                          session_id,
                                                          worker_id,
                                                          test_session.authkey,
                                                          test_session.global_mgr_address,
                                                          test_session.node_mgr_port,)
                                       )
        test_session.worker_procs[worker_id] = proc
        proc.start()
        debug_print(f"Started worker {worker_id} in separate process {proc.pid}")

    @classmethod
    def _cls_ping(cls, address: Tuple[str, int]) -> str:
        return cls._singleton[address].ping()

    # noinspection PyMethodMayBeStatic
    def ping(self) -> str:
        """
        simple ping test when debugging multiprocessing
        """
        return "pong"

    @classmethod
    def _cls_shutdown(cls, address: Tuple[str, int], timeout: Optional[float] = None):
        return cls._singleton[address].shutdown(timeout)

    def shutdown(self, timeout: Optional[float] = None):
        debug_print(f"Shutting down worker agent .. {self._sessions}")
        for session_id in self._sessions.copy():
            self.shutdown_session(session_id, timeout)
        self._sessions = {}
        if self.__class__._mp_server:
            self.__class__._mp_server.shutdown()

    @classmethod
    def _cls_shutdown_session(cls, address: Tuple[str, int], session_id: str, timeout: Optional[float] = None):
        return cls._singleton[address].shutdown_session(session_id=session_id, timeout=timeout)

    def shutdown_session(self, session_id: str, timeout: Optional[float] = None):
        """
        shutdown a session by joining with all worker processes
        :param session_id: which session
        :param timeout: optional timeout value for waiting on a worker, after which the worker will be terminated
            explicitly
        """
        debug_print(f"Worker agent shutting down session {session_id}")
        test_session = self._sessions.get(session_id)
        if self._node_mgr.get(session_id):
            with suppress(Exception):
                self._node_mgr[session_id].stop()
            del self._node_mgr[session_id]
        if test_session:
            for worker_id, worker in test_session.worker_procs.items():
                debug_print(f"Waiting for {worker_id} to terminate... {test_session.worker_procs}")
                worker.join(timeout)
                if worker.exitcode is None:
                    worker.terminate()
                    always_print(f"Terminated worker {worker_id} abruptly", as_error=True)

            test_session.worker_procs = {}
            del self._sessions[session_id]

    @classmethod
    def shutdown_agent(cls, timeout: Optional[float] = None) -> None:
        for instance in cls._singleton.values():
            instance.shutdown(timeout=timeout)
        cls._sessions = {}
        cls._singleton = {}


def main(test_q: JoinableQueue, status_q: JoinableQueue, report_q: JoinableQueue, cwd: Path,
         args: List[str], session_id: str, worker_id: str, authkey: bytes,
         global_mgr_address: Optional[Tuple[str, int]] = None, node_mgr_port: Optional[int] = None):
    from pytest_mproc.worker import WorkerSession  # when working distributed, this is required otherwise singleton will be None
    multiprocessing.current_process().authkey = authkey
    for index, arg in enumerate(args.copy()):
        if arg == '--cores':
            args[index + 1] = '1'
    os.chdir(cwd)
    status_q.put(WorkerStarted(worker_id, os.getpid(), _get_my_ip()))
    if global_mgr_address:
        debug_print(f"Worker {worker_id} connecting to global fixture manager")
        Global.Manager.as_client(global_mgr_address, auth_key=authkey)
    if node_mgr_port:
        debug_print(f"Worker {worker_id} connecting to node fixture manager")
        Node.Manager._port = node_mgr_port
        Node.Manager.as_client(port=node_mgr_port, authkey=authkey)  # start node client to create singleton client
    # noinspection PyUnresolvedReferences
    # from pytest_mproc.worker import WorkerSession  # to make Python happy
    worker = WorkerSession(session_id=session_id,
                           worker_id=worker_id,
                           test_q=test_q,
                           status_q=status_q,
                           report_q=report_q,
                           )
    WorkerSession.set_singleton(worker)
    status = None
    # noinspection PyBroadException
    has_error = False
    msg = f"Worker {worker_id} exited cleanly"
    try:
        args = [a for a in args if not a.startswith('--log-file')]
        args += [f'--log-file={worker_id}/pytest_output.log']
        debug_print(f"Worker {worker_id} calling pytest {args}  {id(WorkerSession)} {WorkerSession.singleton()} {os.getpid()}")
        status = pytest.main(args)
        if isinstance(status, pytest.ExitCode):
            status = status.value
    except Exception as e:
        msg = f"Worker {_get_my_ip()}-{os.getpid()} died with exception {e}\n {traceback.format_exc()}"
        os.write(sys.stderr.fileno(), msg.encode('utf-8'))
        has_error = True
        with suppress(Exception):  # if other end died, status_q is gone, so suppress exceptions
            report_q.put(ClientException(worker_id, e))
            status_q.put(ClientException(worker_id, e))
    else:
        report_q.put(WorkerExited(worker_id, os.getpid(), _get_my_ip(), errored=False, message=msg))
    finally:
        debug_print(f"Worker {worker_id} completed, cleaning up...")
        status_q.put(WorkerExited(worker_id, os.getpid(), _get_my_ip(), errored=has_error, message=msg))
        signal.alarm(120)
        if status != 0:
            always_print(f"Worker {worker_id} exiting {status if status is not None else -1} [{os.getpid()}]",
                         as_error=True)
        else:
            debug_print(f"Worker {worker_id} exiting {status if status is not None else -1} [{os.getpid()}]")
        signal.alarm(0)
    sys.exit(status if status is not None else -1)


if __name__ == "__main__":
    port = _find_free_port()
    authkey_ = sys.stdin.readline()
    sys.stdin.close()
    authkey_ = binascii.a2b_hex(authkey_.strip())
    multiprocessing.current_process().authkey = authkey_
    srvr = WorkerAgent.start_server(address=(_get_my_ip(), port), authkey=authkey_)
    sys.stderr.write(str(port) + '\n')
    sys.stderr.close()
    # serve until agent is shut down explicitly:
    # noinspection PyUnresolvedReferences
    debug_print("Serving client...")
    srvr.join()
    always_print("Worker agent terminated")
