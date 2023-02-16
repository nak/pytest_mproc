import asyncio
import binascii
import multiprocessing
import os
import secrets
import signal
import traceback
from abc import ABC, abstractmethod

from pytest_mproc.mp import JoinableQueue, SharedJoinableQueue
from multiprocessing.managers import BaseManager, SyncManager

import resource
import sys
import time

from contextlib import suppress
from pathlib import Path
from queue import Empty
from typing import List, Optional, Union, Dict, Tuple, Any, Set

import pytest
from _pytest.reports import TestReport

from pytest_mproc import AsyncMPQueue, _get_my_ip, find_free_port
from pytest_mproc.exceptions import FatalError
from pytest_mproc.constants import DEFAULT_PRIORITY, ENV_PTMPROC_ORCHESTRATOR, ENV_PTMPROC_SESSION_ID
from pytest_mproc.data import (
    WorkerExited,
    GroupTag,
    StatusTestsComplete,
    TestBatch,
    StatusTestState,
    TestStateEnum, resource_utilization, WorkerStarted, AllWorkersDone,
)
from pytest_mproc.data import StatusType
from pytest_mproc.fixtures import Global, Node
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc.utils import BasicReporter


__all__ = ["Orchestrator", "MainSession"]

from pytest_mproc.worker import WorkerAgent


class OrchestratorInterface(ABC):

    @abstractmethod
    def shutdown_session(self, on_error: bool = False, timeout: Optional[float] = None):
        """
        Shutdown the session, terminating any ongoing processes and preventing any further
        actions on the session
        """

    @abstractmethod
    def join_session(self, on_error: bool = False, timeout: Optional[float] = None) -> int:
        """
        join a session (generally on termination, to ensure all threads/processes have truly
        ended
        :param on_error: whether joining under occurrence of an error that terminated the session unexpectedly
        :param timeout: if specified, max time to wait
        :raises: TimeoutError if waiting too long
        """

    @abstractmethod
    def conduct_session(self, args: List[str], cwd: Path):
        """
        conduct a (test) session
        :param args: list of args to pass to workers
        :param cwd: path under which session is conducted (on remote worker nodes and locally)
        """

    @abstractmethod
    def start_worker(self, worker_id: str,
                     agent_address: Optional[Tuple[str, int]] = None, agent_authkey: Optional[bytes] = None):
        """
        Start a worker process(thread)
        :param worker_id: id of worker unique to session
        :param agent_address: host/port tuple of worker agent
        :param agent_authkey: auth key of worker agent (for making mp connection)
        """

    @classmethod
    def as_local(cls, session_id: str, has_remote_workers: bool = False) -> "OrchestratorInterface":
        """
        return local-host orchestrator
        :param session_id: unique session id
        :param has_remote_workers: whether remote workers will be present during session
        """
        host, port = ('localhost', find_free_port())
        orchestrator = Orchestrator(has_remote_workers=has_remote_workers, session_id=session_id,
                                    global_mgr_address=(host, port))
        # noinspection PyProtectedMember,PyUnresolvedReferences
        authkey = multiprocessing.current_process().authkey
        Global.Manager.as_server(address=(host, port), auth_key=authkey)
        return orchestrator

    @classmethod
    def as_client(cls, session_id: str, address: Tuple[str, int],
                  has_remote_workers: bool = False) -> "OrchestratorInterface":
        """
        return client (proxy) to a remote orchestrator
        :param session_id: unique session id
        :param address: host/port of remote orchestrator agent (creating the orchestrator instance
        remotely)
        :param has_remote_workers: whether remote workers will be present during session
        """
        return OrchestrationAgent.OrchestratorProxy(session_id=session_id,
                                                    has_remote_workers=has_remote_workers,
                                                    address=address)


class Orchestrator(OrchestratorInterface):
    """
    class that acts as Main point of orchestration
    """

    # noinspection PyUnresolvedReferences
    def __init__(self, session_id: str, global_mgr_address: Tuple[str, int], has_remote_workers: bool = False):
        """
        :param session_id: unique session id
        :param has_remote_workers: has workers from remote host(s)
        """
        self._session_id = session_id
        self._authkey = multiprocessing.current_process().authkey
        os.environ[ENV_PTMPROC_SESSION_ID] = session_id
        # for accumulating tests and exit status:
        self._has_remote_workers = has_remote_workers
        self._worker_queue = self._mp_server.JoinableQueue() if not self._is_local else JoinableQueue()
        self._main: Optional[multiprocessing.Process] = None
        self._global_mgr_address = global_mgr_address
        self._node_mgr_port: int = find_free_port()
        self._node_mgr = Node.Manager.as_server(self._node_mgr_port,
                                                session_id=session_id,
                                                authkey=self._authkey)
        if has_remote_workers:
            self._main_authkey = secrets.token_bytes(64)
            self._main_address = global_mgr_address
            self._sm_base = SyncManager(address=self._main_address, authkey=self._main_authkey)
            self._sm_base.register('JoinableQueue', JoinableQueue,
                                   exposed=['put', 'get', 'close', 'join', 'task_done'])
            self._sm_base.start()
            self._test_q = self._sm_base.JoinableQueue()
            self._status_q = self._sm_base.JoinableQueue()
            self._report_q = self._sm_base.JoinableQueue()
        else:
            self._main_authkey = None
            self._main_address = None
            self._test_q = SharedJoinableQueue()
            self._status_q = SharedJoinableQueue()
            self._report_q = SharedJoinableQueue()
        self._internal_server = None

    @property
    def main_proc(self) -> multiprocessing.Process:
        return self._main

    def conduct_session(self, args: List[str], cwd: Path, ) -> JoinableQueue:
        """
        Conduct a test session
        :param args: args to pass to pytest main
        :param cwd: where to run the session (where all workers will run from)
        :returns: queue to pass reports back to overarching session
        """
        debug_print(f"Launching main session...")
        proc = multiprocessing.Process(target=MainSession.launch,
                                       args=(self._session_id, self._authkey,
                                             self._main_address, self._main_authkey,
                                             self._test_q, self._status_q, self._report_q,
                                             self._worker_queue, args, cwd,
                                             self._node_mgr_port),
                                       )
        if self._authkey is not None:
            proc.authkey = self._authkey
        proc.start()
        debug_print(f"Main session '{self._session_id}' launched [{proc.pid}]")
        self._main = proc
        return self._report_q

    def join_session(self, on_error: bool = False, timeout: Optional[float] = None) -> int:
        with suppress(Exception):
            self._status_q.close()
        with suppress(Exception):
            self._test_q.close()
        if self._main is not None:
            debug_print(f"Joining main session {self._session_id} [{self._main.is_alive()}]")
            for _ in range(10):
                if not self._main.is_alive():
                    break
                time.sleep(0.5)
            self._main.join(timeout=timeout)
            if on_error and self._main.exitcode is None:
                debug_print(f"Sending SIGTERM on error to main session [{self._main.pid}]")
                self._main.terminate()
                self._main.join(timeout=2)
                if self._main.exitcode is None:
                    always_print(
                        f"Forcibly stopped main session on error {self._session_id} [{self._main.exitcode}]",
                        as_error=True
                    )
                    self._main.kill()
                    self._main.join(timeout=2)
                return -1
            elif self._main.exitcode is None:
                always_print(f"Forcibly stopped main session {self._session_id} [{self._main.exitcode}]",
                             as_error=True)
                self._main.kill()
                self._main.join(timeout=2)
            debug_print("Main session done")
            return self._main.exitcode
        return 0

    def shutdown_session(self, on_error: bool = False, timeout: Optional[float] = None) -> int:
        """
        End this session, waiting until main session process exits
        :param on_error: if True, will terminate abruptly
        :param timeout: optional timeout to wait before forcibly closing main session (default is 10 seconds)
        """
        with suppress(BrokenPipeError, TimeoutError):
            self._worker_queue.put(None, block=False, timeout=1)
        self._node_mgr.shutdown()
        if Global.Manager.singleton():
            Global.Manager.singleton().shutdown()
        status = self.join_session(on_error=on_error, timeout=timeout)
        return status

    def start_worker(self, worker_id: str, address: Optional[Tuple[str, int]] = None,
                     agent_authkey: Optional[bytes] = None) -> None:
        """
        Start a new worker under the given test session
        :param worker_id: unique id assigned to worker (unique to session)
        :param address: optional address of worker (or None for simple localhost instantiation)
        :param agent_authkey: authkey for authentication connecting to worker agent
        """
        debug_print(f"Orchestrator signalling start worker {worker_id} at {address}")
        self._worker_queue.put(
            (worker_id, address, self._global_mgr_address, self._node_mgr_port,
             self._authkey.hex() if self._authkey else None,
             agent_authkey)
        )

    def workers_exhausted(self) -> None:
        """
        signal that all workers are exhausted (no more worker will be created)
        """
        with suppress(Exception):
            self._worker_queue.put(None, timeout=1)


class MainSession:
    """
    For conducting main session in a separate process
    """

    _singleton: "MainSession" = None

    @staticmethod
    def launch(session_id: str, authkey: bytes, main_address: Optional[Tuple[int, str]],
               main_authkey: Optional[bytes],
               test_q: JoinableQueue, status_q: JoinableQueue, report_q: JoinableQueue,
               worker_q: JoinableQueue, args: List[str], cwd: Path,
               node_mgr_port: int) -> None:
        os.chdir(cwd)
        multiprocessing.current_process().authkey = authkey
        debug_print(f"Starting main session {session_id}... {args}")
        status = -1
        # noinspection PyProtectedMember
        Node.Manager._ports[session_id] = node_mgr_port
        if main_address:

            def get_test_q():
                nonlocal test_q
                return test_q

            def get_status_q():
                nonlocal status_q
                return status_q

            def get_report_q():
                nonlocal report_q
                return report_q

            m_address = (_get_my_ip(), find_free_port())
            sm = SyncManager(address=m_address, authkey=main_authkey)
            sm.register('get_test_q', get_test_q)
            sm.register('get_status_q', get_status_q)
            sm.register('get_report_q', get_report_q)
            sm.start()
        else:
            m_address = None
        with MainSession(session_id=session_id, main_address=m_address, main_authkey=main_authkey,
                         test_q=test_q, status_q=status_q, report_q=report_q,
                         worker_q=worker_q, args=args, authkey=authkey) as session:
            def handler(*_args, **_kwargs):
                session.shutdown()
                signal.raise_signal(signal.SIGKILL)

            # noinspection PyTypeChecker
            signal.signal(signal.SIGTERM, handler)
            try:
                if all([not arg.startswith("--log-file") for arg in args]):
                    args.append('--log-file=pytest_debug.log')
                debug_print(f"Starting main orchestrator...\n   cmd: pytest {' '.join(args)}\n   from: {cwd}"
                            f"\n    args: {args}")
                os.environ[ENV_PTMPROC_ORCHESTRATOR] = '1'
                os.environ[ENV_PTMPROC_SESSION_ID] = session_id
                status = pytest.main(args)
                worker_q.put(None)  # signals no more workers to be processed/added
                if isinstance(status, pytest.ExitCode):
                    status = status.value
            except Exception as e:
                msg = f"Main execution orchestration died with exit code {status}: {e} [{type(e)}]"
                os.write(sys.stderr.fileno(), msg.encode('utf-8'))
                status = -1

        with suppress(Exception):
            report_q.put(None)
        debug_print(f"\nSession {session_id} exiting  [{os.getpid()}]")
        assert session._count == session._target_count, \
            f"Failed to execute {session._target_count - session._count} out of {session._target_count} tests"
        sys.exit(status)

    @classmethod
    def singleton(cls):
        return cls._singleton

    def __init__(self, session_id: str, main_address, main_authkey,
                 test_q: JoinableQueue, status_q: JoinableQueue, report_q: JoinableQueue,
                 worker_q: JoinableQueue, args: List[str], authkey: bytes):
        if MainSession._singleton is not None:
            raise RuntimeError("More than one instance of MainSession is not allowed")
        MainSession._singleton = self
        self._main_address = main_address
        self._main_authkey = main_authkey
        self._session_id = session_id
        self._test_q = AsyncMPQueue(test_q)
        self._status_q = AsyncMPQueue(status_q)
        self._report_q = AsyncMPQueue(report_q)
        self._worker_q = worker_q
        self._pending: Dict[str, StatusTestState] = {}
        self._exit_results: List[StatusTestsComplete] = []
        self._count = 0
        self._target_count = 0
        self._authkey = authkey
        self._session_start_time = time.time()
        # for information output:
        self._reporter = BasicReporter()
        self._rusage = None
        self._active = False
        self._test_end_signalled = False
        self._all_workers_done = False
        self._worker_agents: Dict[Union[Tuple[str, int], WorkerAgent]] = {}
        self._worker_polling_proc: Optional[multiprocessing.Process] = None
        self._workers: Set[str] = set()
        self._pytest_args = args
        self._tests_remain = True
        self._worker_polling_sem = multiprocessing.Semaphore(0)
        self._worker_agents: Dict[str, List[WorkerAgent]] = {}
        self._worker_agents_by_address: Dict[Tuple[str, int], WorkerAgent] = {}

    @staticmethod
    def _write_sep(s, txt):
        """
        write out text to stdout surrounded by repeated character

        :param s: character to repeat on either side
        :param txt: text to be surrounded
        """
        sep_total = max((70 - 2 - len(txt)), 2)
        sep_len = sep_total // 2
        sep_extra = sep_total % 2
        out = '%s %s %s\n' % (s * sep_len, txt, s * (sep_len + sep_extra))
        sys.stdout.write(out)

    def _output_summary(self):
        """
        Output the summary of test execution
        """
        sys.stdout.write("User CPU, System CPU utilization, Additional memory during run\n")
        sys.stdout.write("---------------------------------------------------------\n")
        for exit_result in self._exit_results:
            if exit_result.test_count > 0:
                sys.stdout.write(
                    f"Process {exit_result.worker_id} executed " +
                    f"{exit_result.test_count} tests in {exit_result.resource_utilization.time_span:.2f} " +
                    f"seconds; User CPU: {exit_result.resource_utilization.user_cpu:.2f}%, " +
                    f"Sys CPU: {exit_result.resource_utilization.system_cpu:.2f}%, " +
                    f"Mem consumed (additional from base): "
                    f"{exit_result.resource_utilization.memory_consumed / 1000.0:.2f}M\n")
            else:
                sys.stdout.write(f"Process {exit_result.worker_id} executed 0 tests\n")
        sys.stdout.write("\n")
        if self._rusage:
            time_span = self._rusage.time_span
            ucpu = self._rusage.user_cpu
            scpu = self._rusage.system_cpu
            unshared_mem = self._rusage.memory_consumed
            self._write_sep('=', "STATS")
            sys.stdout.write(
                f"Process Orchestrator executed in {time_span:.2f} seconds. " +
                f"User CPU: {ucpu:.2f}%, Sys CPU: {scpu:.2f}%, " +
                f"Mem consumed: {unshared_mem / 1000.0}M\n\n"
            )
        if self._count != self._target_count:
            self._write_sep(
                '!', "{} tests unaccounted for {} out of {}".format(self._target_count - self._count,
                                                                    self._count, self._target_count))

    async def _process_worker_message(self, result: StatusType):
        """
        Process a message (as a worker) from the coordinating process

        :param result: the result to process
        """
        try:
            if isinstance(result, StatusTestsComplete):
                # process is complete, so close it and set to None
                self._exit_results.append(result)
            elif isinstance(result, WorkerExited):
                always_print(f"Worker {result.worker_id} has exited", as_error=True)
                self._workers.remove(result.worker_id)
                if len(self._workers) == 0:
                    always_print(f"All workers have exited")
            elif isinstance(result, Exception):
                raise Exception("Internal ERROR") from result
            elif result is None:
                pass
            elif isinstance(result, StatusTestState):
                if result.state == TestStateEnum.STARTED:
                    self._pending[result.test_id] = result
                elif result.state == TestStateEnum.FINISHED:
                    self._count += 1
                    with suppress(Exception):
                        del self._pending[result.test_id]
                elif result.state == TestStateEnum.RETRY:
                    with suppress(Exception):
                        del self._pending[result.test_id]
                    await self._test_q.put(TestBatch(test_ids=[result.test_id]))
            else:
                raise Exception(f"Internal Error: Unknown result type: {type(result)}!!")

        except Exception as e:
            sys.stdout.write("INTERNAL_ERROR> %s\n" % str(e))

    async def _process_worker_died(self, message: WorkerExited):
        """
        process a message that a client has died
        :param message: message to process
        """
        key = f"{message.host}-{message.pid}"
        # noinspection PyUnresolvedReferences
        # self._mp_manager.completed(message.host, message.pid)
        if message.errored:
            for test_id, test_state in self._pending.copy().items():
                if test_state.host != message.host:
                    continue
                del self._pending[test_id]
                if len(test_state.test_batch.test_ids) == 1 \
                        or test_state.test_id == test_state.test_batch.test_ids[0]:
                    if not self._test_end_signalled:
                        # reschedule by putting back in test queue
                        await self._test_q.put(test_state.test_batch)
                    else:
                        always_print(f"!!! Unable to reschedule test {test_id} !!!")
                else:
                    always_print(f"Skipping test batch '{test_state.test_batch.test_ids}'"
                                 " due to client fault; cannot reschedule a partially completed test batch")
                    index = test_state.test_batch.test_ids.index(test_id)
                    for tid in test_state.test_batch.test_ids[index:]:
                        await self._status_q.put([
                            TestReport(nodeid=tid,
                                       location=("<<unknown>>", None, "<<unknown>>"),
                                       keywords={},
                                       outcome='skipped',
                                       when='call',
                                       longrepr=f"Host {test_state.host} or process on host became  "
                                                "unresponsive or died.  Test cannot be retried as it "
                                                " is part of a batch"
                                       )])
            always_print(f"\nA worker {key} has died: {message.message}\n")
        else:
            # noinspection PyUnresolvedReferences
            debug_print(f"\nWorker-{key} finished\n")

    async def _read_results(self):
        """
        read results and take actions (in batches)
        """
        exceptions = []
        success = True
        try:
            test_count = 0
            while True:
                result_batch: Union[List[StatusType], None] = await self._status_q.get()
                if isinstance(result_batch, WorkerExited):
                    self._workers.remove(result_batch.worker_id)
                    await self._process_worker_died(result_batch)
                    self._all_workers_done = len(self._workers) == 0
                    if self._all_workers_done:
                        break
                elif isinstance(result_batch, WorkerStarted):
                    self._workers.add(result_batch.worker_id)
                    debug_print(f"Worker {result_batch.worker_id} started")
                    if not self._tests_remain:
                        await self._test_q.put(None)  # ensures new worker exits at right time
                elif isinstance(result_batch, Exception):
                    always_print(f"Exception from remote worker: {result_batch}", as_error=True)
                    exceptions.append(result_batch)
                elif isinstance(result_batch, StatusTestsComplete):
                    self._exit_results.append(result_batch)
                elif isinstance(result_batch, list):
                    for result in result_batch:
                        if isinstance(result, Exception):
                            raise Exception("Internal ERROR") from result
                        else:
                            test_count += 1
                            await self._process_worker_message(result)
                elif result_batch is None:
                    always_print("Exiting results processing on sentinel")
                    break
                if len(self._workers) == 0:
                    break
            # process stragglers left in results queue
            try:
                result_batch = await self._status_q.get(immediate=True)
                while result_batch:
                    if isinstance(result_batch, Exception):
                        exceptions.append(result_batch)
                    elif isinstance(result_batch, list):
                        for result in result_batch:
                            await self._process_worker_message(result)
                    elif isinstance(result_batch, StatusTestsComplete):
                        self._exit_results.append(result_batch)
                    elif result_batch is None:
                        break
                    result_batch = await self._status_q.get(immediate=True)
            except Empty:
                pass
            await self._report_q.put(AllWorkersDone())
        except KeyboardInterrupt:
            always_print("\n!!! Testing interrupted reading of results !!!")
            raise
        except Exception as e:
            traceback.print_exc()
            always_print(f"Fatal exception during results processing, shutting down: {e}", as_error=True)
            raise
        finally:
            if exceptions:
                always_print(f"One or more remote worker exceptions encountered during execution", as_error=True)
                for e in exceptions:
                    always_print(f"EXCEPTION: {e} [{type(e)}]", as_error=True)
            return success

    async def _populate_test_queue(self, tests: List[TestBatch]) -> None:
        """
        Populate test queue (asynchronously) given the set of tests
        :param tests: complete set of tests
        """
        always_print("Populating tests and waiting for workers...")
        try:
            count = 0
            for test_batch in tests:
                # Function objects in pytest are not pickle-able, so have to send string node-id and
                # do lookup on worker side
                await self._test_q.put(test_batch)
                count += 1
                if not self._active:
                    break
            debug_print(f"{self} All tests served, exiting test loop with {len(self._workers)} workers")
            self._tests_remain = False
            for _ in range(len(self._workers)):
                await self._test_q.put(None)
        except (EOFError, BrokenPipeError, ConnectionError, KeyboardInterrupt) as e:
            always_print(f"!!! Result processing interrupted;  stopping queue !!!: {e}")
        except Exception as e:
            always_print(f"!!! Exception while populating tests; shutting down: {e} !!!")
        else:
            always_print(f"All tests have been queued for execution")

    # noinspection PyProtectedMember
    @staticmethod
    def _sorted_tests(tests) -> Tuple[List[TestBatch], List[Any]]:
        """
        :param tests: the items containing the pytest hooks to the tests to be run
        """
        skipped_tests = []
        valid_tests = []
        for t in tests:
            # noinspection SpellCheckingInspection
            if (t.keywords.get('skipif') and t.keywords.get('skipif').args[0]) or t.keywords.get('skip'):
                skipped_tests.append(t)
            else:
                valid_tests.append(t)
        tests = valid_tests

        # noinspection PyProtectedMember
        def priority(test_) -> int:
            tag_ = getattr(test_._pyfuncitem.obj, "_pytest_group", None)
            tag_priority = tag_.priority if tag_ is not None else DEFAULT_PRIORITY
            return getattr(test_._pyfuncitem.obj, "_pytest_priority", tag_priority)

        grouped = [t for t in tests if getattr(t._pyfuncitem.obj, "_pytest_group", None)
                   or getattr(t._pyfuncitem, "_pytest_group", None)]
        tests = [TestBatch([t.nodeid.split(os.sep)[-1]], priority(t)) for t in tests if t not in grouped]
        groups: Dict["GroupTag", TestBatch] = {}
        for test in grouped:
            tag = test._pyfuncitem.obj._pytest_group if hasattr(test._pyfuncitem.obj, "_pytest_group") \
                else test._pyfuncitem._pytest_group
            groups.setdefault(tag, TestBatch([], priority(test))).test_ids.append(test)
        for tag, group in groups.items():
            # noinspection PyUnresolvedReferences
            groups[tag].test_ids = [test.nodeid for test in sorted(group.test_ids, key=lambda x: priority(x))]
            groups[tag].restriction = tag.restrict_to
        tests.extend(groups.values())
        tests = sorted(tests, key=lambda x: x.priority)
        return tests, skipped_tests

    def _poll_for_workers(self, sem: multiprocessing.Semaphore):
        debug_print(f"\nPolling for workers... {self}")
        worker_agent_and_token = self._worker_q.get()
        debug_print(f"Got worker {worker_agent_and_token}")
        count = 0
        while worker_agent_and_token is not None:
            worker_id, address, global_mgr_address, node_mgr_port, authkey, agent_authkey = \
                worker_agent_and_token
            authkey = binascii.a2b_hex(authkey)
            real_address = address if address is not None else ('localhost', 0)
            if real_address not in self._worker_agents_by_address:
                if address is not None:
                    retries = 3
                    delay = 0.5
                    worker_agent = None
                    while True:
                        try:
                            worker_agent = WorkerAgent.as_client(address, authkey=agent_authkey)
                            break
                        except (ConnectionError, ConnectionRefusedError) as e:
                            retries -= 1
                            if retries == 0:
                                always_print(f"ERROR: Failed to get client for worker-agent: {e}")
                                break
                            time.sleep(delay)
                            delay *= 1.5
                        except BaseException as e:
                            always_print(f"Exception in connecting to worker agent at {address}: {e}")
                else:
                    worker_agent = WorkerAgent.as_local()
                if worker_agent is None:
                    worker_agent_and_token = self._worker_q.get()
                    continue
                self._worker_agents_by_address[real_address] = worker_agent
                if self._main_address is not None:
                    test_q, status_q, report_q = None, None, None
                else:
                    test_q, status_q, report_q = self._test_q, self._status_q, self._report_q
                worker_agent.start_session(
                    session_id=self._session_id,
                    main_address=self._main_address,
                    main_token=self._main_authkey.hex() if self._main_authkey else None,
                    test_q=test_q.raw() if self._main_address is None else None,
                    status_q=status_q.raw() if self._main_address is None else None,
                    report_q=report_q.raw() if self._main_address is None else None,
                    args=self._pytest_args,
                    cwd=Path(os.getcwd()).absolute(),
                    global_mgr_address=global_mgr_address,
                    node_mgr_port=node_mgr_port,
                    token=authkey.hex() if authkey else None
                )
                self._worker_agents.setdefault(self._session_id, []).append(worker_agent)
            else:
                worker_agent = self._worker_agents_by_address[real_address]
            try:
                debug_print(f"Got worker {worker_id}")
                debug_print(f"Requesting start worker {self._session_id}@{worker_id}")
                worker_agent.start_worker(session_id=self._session_id, worker_id=worker_id)
                debug_print(f"Requested worker started")
                count += 1
            except ConnectionError:
                always_print(f"ERROR: Unable to connect to worker agent at {real_address}.  Is it running?",
                             as_error=True)
            except Exception as e:
                import traceback
                always_print(f"ERROR: starting worker: {e}\n{traceback.format_exc()}", as_error=True)
            worker_agent_and_token = self._worker_q.get()
            debug_print(f"Got worker {worker_agent_and_token}")
        debug_print(f"No longer polling for workers [{count}]")
        if count == 0:
            self._all_workers_done = True
            self._status_q.raw().put(None)
            self._report_q.raw().put(AllWorkersDone())
            always_print("Unable to connect to any worker agent or acquire any worker", as_error=True)
        sem.acquire()

    async def run_loop(self, session, tests):
        """
        Populate test queue and continue to process messages from worker Processes until they complete

        :param session: Pytest test session, to get session or config information
        :param tests: the items containing the pytest hooks to the tests to be run
        """
        self._active = True
        test_batches, skipped_tests = self._sorted_tests(tests)
        self._target_count = len(tests) - len(skipped_tests)
        for item in skipped_tests:
            item.config.hook.pytest_runtest_protocol(item=item, nextitem=None)
        if not tests:
            return
        start_rusage = resource.getrusage(resource.RUSAGE_SELF)
        start_time = time.time()
        populate_tests_task = asyncio.create_task(
            self._populate_test_queue(test_batches)
        )
        try:
            # only master will read results and post reports through pytest
            await self._read_results()
        except WorkerExited:
            os.write(sys.stderr.fileno(), b"\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
            os.write(sys.stderr.fileno(), b"All clients died; Possible incomplete run\n")
            os.write(sys.stderr.fileno(), b"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n")
        except FatalError:
            raise session.Failed(True)
        except (EOFError, ConnectionError, BrokenPipeError, KeyboardInterrupt) as e:
            always_print(f"Testing interrupted; shutting down: {type(e)}")
            populate_tests_task.cancel()
        finally:
            debug_print("Exiting orchestration run loop.  Cleaning up...")
            self._active = False
            with suppress(Exception):
                # should never time out since workers are done
                await asyncio.wait_for(populate_tests_task, timeout=1)
            if not populate_tests_task.done():
                populate_tests_task.cancel()
            if not self._all_workers_done:
                always_print(f"Results loop exiting without receiving signal that all workers are done",
                             as_error=True)
            with suppress(Exception):
                populate_tests_task.cancel()
            with suppress(Exception):
                end_rusage = resource.getrusage(resource.RUSAGE_SELF)
                time_span = time.time() - start_time
                self._rusage = resource_utilization(time_span=time_span,
                                                    start_rusage=start_rusage,
                                                    end_rusage=end_rusage)
                sys.stdout.write("\r\n")
            await self._report_q.put(None)
            debug_print("Run loop done")

    def __enter__(self):
        self._worker_polling_proc = multiprocessing.Process(target=self._poll_for_workers,
                                                            args=(self._worker_polling_sem, ))
        self._worker_polling_proc.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        self._output_summary()

    def shutdown(self) -> None:
        """
        shutdown services
        """
        with suppress(Exception):
            self._status_q.raw().put(None)  # sentinel, if needed
        with suppress(Exception):
            self._worker_q.put(None, timeout=1)
        self._worker_polling_sem.release()
        self._worker_polling_proc.join()
        for agent in self._worker_agents_by_address.values():
            agent.shutdown(timeout=3)
        self._worker_agents = {}
        self._worker_agents_by_address = {}
        self._pending = {}
        self._status_q.close()
        self._test_q.close()


class OrchestrationAgent:

    _mp_servers: Dict[Tuple[str, int], BaseManager] = {}
    _instances: Dict[str, "Orchestrator"] = {}

    @classmethod
    def start(cls, address: Tuple[str, int]):
        if address in cls._mp_servers:
            raise RuntimeError("Attempt to start agent on host twice")
        cls._mp_servers[address] = SyncManager(address)
        mp_server = cls._mp_servers[address]
        mp_server.register('create_orchestrator', cls._cls_create_orchestrator)
        mp_server.register('shutdown_session', cls._cls_shutdown_session)
        mp_server.register('conduct_session', cls._cls_conduct_session)
        mp_server.register('start_worker', cls._cls_start_worker)
        mp_server.register('join_session', cls._cls_join_session)

    @classmethod
    def _cls_shutdown(cls, address: Tuple[str, int]):
        cls._mp_servers[address].shutdown()

    class OrchestratorProxy(OrchestratorInterface):

        def __init__(self, session_id: str, address: Tuple[str, int], has_remote_workers: bool = False):
            self._session_id = session_id
            self._address = address
            self._mp_client = SyncManager(address)
            self._mp_client.register('shutdown_session')
            self._mp_client.register('create_orchestrator')
            self._mp_client.register('conduct_session')
            self._mp_client.register('start_worker')
            self._mp_client.register('join_session')
            self._mp_client.connect()
            # will raise RuntimeError if session id already exists (on that host/port agent)
            # noinspection PyUnresolvedReferences
            self._mp_client.create_orchestrator(session_id, has_remote_workers, address)

        def shutdown_session(self, on_error: bool = False, timeout: Optional[float] = None):
            # noinspection PyUnresolvedReferences
            self._mp_client.shutdown_session(self._session_id, on_error, timeout)
            self._mp_client.shutdown()

        def join_session(self, on_error: bool = False, timeout: Optional[float] = None):
            # noinspection PyUnresolvedReferences
            self._mp_client.join_session(self._session_id, on_error, timeout)

        def conduct_session(self, args: List[str], cwd: Path):
            # noinspection PyUnresolvedReferences
            return self._mp_client.conduct_session(session_id=self._session_id, args=args, cwd=cwd)

        def start_worker(self, worker_id: str,
                         agent_address: Optional[Tuple[str, int]] = None,
                         agent_authkey: Optional[bytes] = None):
            # noinspection PyUnresolvedReferences
            return self._mp_client.start_worker(self._session_id, worker_id, agent_address, agent_authkey)

    @classmethod
    def _cls_create_orchestrator(cls, session_id: str, has_remote_workers: bool = False):
        host, port = _get_my_ip(), find_free_port()
        if session_id in cls._instances:
            raise RuntimeError(f"Session with id '{session_id}' already exists")
        cls._instances[session_id] = Orchestrator(session_id=session_id, has_remote_workers=has_remote_workers,
                                                  global_mgr_address=(host, port))
        authkey = multiprocessing.current_process().authkey
        Global.Manager.as_server(address=(host, port), auth_key=authkey)

    @classmethod
    def _cls_conduct_session(cls, session_id: str, args: List[str], cwd: Path):
        instance = cls._instances[session_id]
        return instance.conduct_session(args=args, cwd=cwd)

    @classmethod
    def _cls_shutdown_session(cls, session_id: str, on_error: bool = False, timeout: Optional[float] = None):
        instance = cls._instances[session_id]
        return instance.shutdown_session(on_error=on_error, timeout=timeout)

    @classmethod
    def _cls_join_session(cls, session_id: str, on_error: bool = False, timeout: Optional[float] = None):
        instance = cls._instances[session_id]
        return instance.join_session(on_error=on_error, timeout=timeout)

    @classmethod
    def _cls_start_worker(cls, session_id: str, worker_id: str,
                          agent_address: Optional[Tuple[str, int]] = None, agent_authkey: Optional[bytes] = None):
        instance = cls._instances[session_id]
        return instance.start_worker(worker_id=worker_id, address=agent_address, agent_authkey=agent_authkey)

    @classmethod
    def as_client(cls, session_id: str, address: Tuple[str, int]) -> "OrchestratorProxy":
        """
        :param session_id: unique session id of test session (and orchestrator unique to that session)
        :param address: address to connect to
        :return: Orchestrator proxy to remote instance
        """
        return cls.OrchestratorProxy(session_id, address)
