import asyncio
import binascii
import multiprocessing
import os
import secrets
from dataclasses import dataclass
from multiprocessing import JoinableQueue
from multiprocessing.managers import BaseManager

import resource
import sys
import time

from contextlib import suppress
from pathlib import Path
from queue import Empty
from typing import List, Optional, Union, Dict, Tuple, Any, Set

import pytest
from _pytest.reports import TestReport

from pytest_mproc import AsyncMPQueue, _get_my_ip, _find_free_port
from pytest_mproc.exceptions import FatalError
from pytest_mproc.constants import DEFAULT_PRIORITY
from pytest_mproc.data import (
    WorkerExited,
    GroupTag,
    StatusTestsComplete,
    TestBatch,
    StatusTestState,
    TestStateEnum, resource_utilization, WorkerStarted, AllWorkersDone,
)
from pytest_mproc.data import StatusType
from pytest_mproc.fixtures import Global
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc.utils import BasicReporter


__all__ = ["Orchestrator", "Config", "MainSession"]

from pytest_mproc.worker import WorkerAgent


@dataclass
class Config:
    global_mgr_address: Optional[Tuple[str, int]] = None


class Orchestrator(BaseManager):
    """
    class that acts as Main point of orchestration
    """

    # noinspection PyUnresolvedReferences
    def __init__(self,
                 address: Optional[Tuple[str, int]] = None, authkey: Optional[bytes] = None):
        """
        :param address: address hosting this orchestrator agent, if distributed on multiple hosts
        """
        # for accumulating tests and exit status:
        if address is not None:
            super().__init__(address, authkey=authkey)
            self._is_local = False
        else:
            self._is_local = True
        self._authkey = authkey if authkey else secrets.token_bytes(64)
        self._worker_queues: Dict[str, JoinableQueue] = {}
        self._sessions: Dict[str, multiprocessing.Process] = {}
        mp = multiprocessing.Manager()
        self._status_q: JoinableQueue = mp.JoinableQueue()
        self._test_q: JoinableQueue = mp.JoinableQueue()

    @classmethod
    def as_local(cls) -> "Orchestrator":
        orchestrator = Orchestrator()
        return orchestrator

    @classmethod
    def as_client(cls, address: Tuple[str, int], authkey: bytes) -> "Orchestrator":
        orchestrator = Orchestrator(address=address, authkey=authkey)
        orchestrator.register("start_session", )
        orchestrator.register("shutdown_session", )
        orchestrator.register("start_worker", )
        orchestrator.register("sessions", )
        orchestrator.register("join_session", )
        orchestrator.connect()
        return orchestrator

    @classmethod
    def as_server(cls, address: Tuple[str, int], authkey: bytes) -> "Orchestrator":
        orchestrator = Orchestrator(address=address, authkey=authkey)
        orchestrator.register("start_session", orchestrator.start_session)
        orchestrator.register("shutdown_session", orchestrator.shutdown_session)
        orchestrator.register("start_worker", orchestrator.start_worker)
        orchestrator.register("sessions", orchestrator.sessions)
        orchestrator.register("join_session", orchestrator.join_session)
        orchestrator.start()
        return orchestrator

    def start_globals(self, return_data: Config):
        if self._is_local:
            global_mgr_address = ('localhost', _find_free_port())
        else:
            global_mgr_address = (_get_my_ip(), _find_free_port())
        Global.Manager.as_server(address=global_mgr_address, auth_key=self._authkey)
        return_data.global_mgr_address = global_mgr_address

    def sessions(self) -> List[str]:
        return list(self._sessions.keys())

    def start_session(self,
                      session_id: str,
                      report_q: JoinableQueue,
                      args: List[str],
                      token: str,
                      cwd: Path = Path(os.getcwd()),
                      ) -> None:
        """
        Conduct a test session
        :param session_id: unique session id associated with test session
        :param args: args to pass to pytest main
        :param token: communication token
        :param cwd: where to run the session (where all workers will run from)
        :param report_q: queue to pass reports back to overarching session
        """
        assert self._test_q is not None
        if session_id in self._sessions:
            raise ValueError(f"Session {session_id} already  actively exists")
        # noinspection PyUnresolvedReferences
        worker_q = multiprocessing.Manager().JoinableQueue()
        self._worker_queues[session_id] = worker_q
        proc = multiprocessing.Process(target=MainSession.launch,
                                       args=(session_id, token, self._test_q, self._status_q, report_q,
                                             worker_q, args, cwd),
                                       )
        proc.authkey = binascii.a2b_hex(token)
        proc.start()
        debug_print(f"Main session started {token}")
        self._sessions[session_id] = proc

    def join_session(self, session_id: str, timeout: Optional[float] = None) -> int:
        if session_id not in self._sessions:
            raise ValueError(f"No such session: {session_id}")
        debug_print(f"Joining main session {session_id} [{self._sessions[session_id].is_alive()}]")
        for _ in range(10):
            if not self._sessions[session_id].is_alive():
                break
            time.sleep(0.5)
        self._sessions[session_id].join(timeout=timeout)
        debug_print("Main session done")
        return self._sessions[session_id].exitcode

    def shutdown_session(self, session_id: str, timeout: Optional[float] = 10.0) -> Dict[str, int]:
        """
        End a session, waiting until main session process exits
        :param session_id: unique session id associated with test session
        :param timeout: optional timeout to wait before forcibly closing main session (default is 10 seconds)
        """
        if Global.Manager.singleton():
            Global.Manager.singleton().shutdown()
        end_time = time.monotonic() + timeout
        if session_id in self._worker_queues:
            with suppress(BrokenPipeError):
                self._worker_queues[session_id].put(None)
            self._worker_queues[session_id].close()
            del self._worker_queues[session_id]
        self._test_q.join()
        self._test_q.close()
        self._status_q.join()
        self._status_q.close()
        if session_id in self._sessions:
            proc = self._sessions[session_id]
            del self._sessions[session_id]
            timeout = end_time - time.monotonic()
            if timeout <= 0:
                timeout = None
            proc.join(timeout=timeout)
            if proc.exitcode is None:
                proc.terminate()
                always_print(f"Forcibly stopped main session")
            return {'exitcode': proc.exitcode}

    def start_worker(self, session_id: str, worker_id: str, address: Optional[Tuple[str, int]] = None) -> None:
        """
        Start a new worker under the given test session
        :param session_id: which test session
        :param worker_id: unique id assigned to worker (unique to session)
        :param address: optional address of worker (or None for simple localhost instantiation)
        """
        if session_id not in self._sessions:
            raise ValueError(f"No such session to start worker: {session_id}")
        session = self._sessions[session_id]
        worker_q = self._worker_queues[session_id]
        debug_print(f"Orchestrator signalling start worker {worker_id} at {address} with token {session.authkey.hex()}")
        worker_q.put((worker_id, address, session.authkey.hex()))

    # to make mypy/type checking happy
    JoinableQueue = JoinableQueue
    Config = Config


class MainSession:
    """
    For conducting main session in a separate process
    """

    _singleton: "MainSession" = None

    @staticmethod
    def launch(session_id: str, token: str, test_q: JoinableQueue, status_q: JoinableQueue, report_q: JoinableQueue,
               worker_q: JoinableQueue, args: List[str], cwd: Path) -> None:
        os.chdir(cwd)
        debug_print(f"Launched session {session_id}")
        with MainSession(session_id=session_id, test_q=test_q, status_q=status_q, report_q=report_q,
                         worker_q=worker_q, args=args, authkey=binascii.a2b_hex(token)):
            try:
                if all([not arg.startswith("--log-file") for arg in args]):
                    args.append('--log-file=pytest_debug.log')
                debug_print(f"Starting main orchestrator...\n   cmd: pytest {' '.join(args)}\n   from: {cwd}"
                            f"\n    args: {args}")
                status = pytest.main(args)
                worker_q.put(None)  # signals no more workers to be processed/added
                if isinstance(status, pytest.ExitCode):
                    status = status.value
            except Exception as e:
                msg = f"Main execution orchestration died with exit code {status}: {e} [{type(e)}]"
                os.write(sys.stderr.fileno(), msg.encode('utf-8'))
                status = -1
        always_print(f">>>>>>>>>>>>>>>>>>> EXITING WITH STATUS {status} [{os.getpid()}]", as_error=True)
        sys.exit(status)

    @classmethod
    def singleton(cls):
        return cls._singleton

    def __init__(self, session_id: str, test_q: JoinableQueue, status_q: JoinableQueue, report_q: JoinableQueue,
                 worker_q: JoinableQueue, args: List[str], authkey: bytes):
        if MainSession._singleton is not None:
            raise RuntimeError("More than one instance of MainSession is not allowed")
        MainSession._singleton = self
        self._session_id = session_id
        self._test_q = AsyncMPQueue(test_q)
        self._status_q = AsyncMPQueue(status_q)
        self._worker_q = worker_q
        self._report_q = AsyncMPQueue(report_q)
        self._pending: Dict[str, StatusTestState] = {}
        self._exit_results: List[StatusTestsComplete] = []
        self._count = 0
        self._target_count = 0
        self._authkey = authkey
        self._session_start_time = time.time()
        # for information output:
        self._reporter = BasicReporter()
        self._rusage = None
        # self._test_q = AsyncMPQueue(wrapped_q=test_q)
        # self._status_q = AsyncMPQueue(wrapped_q=status_q)
        self._active = False
        self._test_end_signalled = False
        self._all_workers_done = False
        self._worker_agents: Dict[Union[Tuple[str, int], WorkerAgent]] = {}
        self._worker_polling_proc: Optional[multiprocessing.Process] = None
        self._workers: Set[str] = set()
        self._pytest_args = args
        self._tests_remain = True

    @staticmethod
    def _write_sep(s, txt):
        """
        write out text to stdout surrounded by repeated character

        :param s: character to repeat on either side of given text
        :param txt: text to by surrounded
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
        try:
            test_count = 0
            while not self._all_workers_done:
                result_batch: Union[List[StatusType], None] = await self._status_q.get()
                if result_batch is None:
                    always_print(f">>> Premature ending found in results queue.<<<<<", as_error=True)
                    self._all_workers_done = True
                    break
                if isinstance(result_batch, WorkerExited):
                    self._workers.remove(result_batch.worker_id)
                    await self._process_worker_died(result_batch)
                elif isinstance(result_batch, WorkerStarted):
                    self._workers.add(result_batch.worker_id)
                    always_print(f"Worker {result_batch.worker_id} added {self._tests_remain}", as_error=True)
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
                    result_batch = await self._status_q.get(immediate=True)
            except Empty:
                pass
            await self._report_q.put(AllWorkersDone())
        except KeyboardInterrupt:
            always_print("\n!!! Testing interrupted reading of results !!!")
            raise
        except Exception as e:
            always_print(f"Fatal exception during results processing, shutting down: {e}", as_error=True)
            raise
        finally:
            if exceptions:
                always_print(f"One or more remote worker exceptions encountered during execution", as_error=True)
                for e in exceptions:
                    always_print(f"EXCEPTION: {e} [{type(e)}]", as_error=True)

    async def _populate_test_queue(self, tests: List[TestBatch]) -> None:
        """
        Populate test queue (asynchronously) given the set of tests
        :param tests: complete set of tests
        """
        always_print("Populating tests and waiting for workers...")
        try:
            count = 0
            for test_batch in tests:
                # Function objects in pytest are not pickle-able, so have to send string nodeid and
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
            self._test_q.close()
        except Exception as e:
            always_print(f"!!! Exception while populating tests; shutting down: {e} !!!")
            self._test_q.close()
        else:
            self._test_q.close()
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

    def _poll_for_workers(self):
        debug_print("\nPolling for workers... ")
        worker_address_and_token = self._worker_q.get()
        worker_agents = {}
        while worker_address_and_token is not None:
            try:
                worker_id, worker_address, authkey = worker_address_and_token
                real_address = worker_address or 'localhost'
                debug_print(f"Got worker {worker_id}@{real_address}")
                authkey = binascii.a2b_hex(authkey)
                if real_address not in worker_agents:
                    debug_print(f"Requesting worker at {worker_address}")
                    agent = WorkerAgent.as_client(address=worker_address, authkey=authkey) \
                        if worker_address is not None\
                        else WorkerAgent.as_local()
                    worker_agents[real_address] = agent
                    agent.start_session(session_id=self._session_id,
                                        test_q=self._test_q.raw(),
                                        status_q=self._status_q.raw(), report_q=self._report_q.raw(),
                                        args=self._pytest_args,
                                        cwd=Path(os.getcwd()).absolute(),
                                        token=self._authkey)
                agent = worker_agents[real_address]
                agent.start_worker(session_id=self._session_id, worker_id=worker_id)
                worker_address_and_token = self._worker_q.get()
            except Exception as e:
                always_print(f"ERROR: starting worker: {e}")
                worker_address_and_token = None

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
            self.shutdown()
        finally:
            debug_print("Exiting orchestration run loop.  Cleaning up...")
            self._active = False
            with suppress(Exception):
                self._status_q.close()
            with suppress(Exception):
                # should never time out since workers are done
                await asyncio.wait_for(populate_tests_task, timeout=1)
            with suppress(Exception):
                if not self._all_workers_done:
                    self._test_q.close()
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
            debug_print("Run loop done")

    def __enter__(self):
        debug_print("Starting worker polling process...")
        self._worker_polling_proc = multiprocessing.Process(target=self._poll_for_workers)
        self._worker_polling_proc.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        self._output_summary()

    def shutdown(self):
        """
        shutdown services
        """
        if self._worker_polling_proc and self._worker_polling_proc.is_alive():
            self._worker_polling_proc.kill()
        with suppress(Exception):
            self._test_q.close()
        with suppress(Exception):
            self._status_q.close()
        self._pending = {}


# now really set them to what we want:
Orchestrator.register("JoinableQueue", JoinableQueue)
Orchestrator.register("Config", Config)
