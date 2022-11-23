import asyncio
import multiprocessing
import os
from multiprocessing.managers import BaseManager

import resource
import sys
import time

from contextlib import suppress
from queue import Empty
from typing import List, Optional, Union, Dict, Tuple, Any

from _pytest.reports import TestReport

from pytest_mproc import AsyncMPQueue
from pytest_mproc.exceptions import FatalError
from pytest_mproc.constants import DEFAULT_PRIORITY
from pytest_mproc.data import (
    ClientDied,
    GroupTag,
    ResultExit,
    TestBatch,
    TestState,
    TestStateEnum, ReportStarted, ReportFinished, resource_utilization,
)
from pytest_mproc.data import ResultException, ResultTestStatus, ResultType
from pytest_mproc.fixtures import Global
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc.utils import BasicReporter


__all__ = ["Orchestrator"]


class Orchestrator(BaseManager):
    """
    class that acts as Main point of orchestration
    """

    def __init__(self, test_q: multiprocessing.JoinableQueue, results_q: multiprocessing.JoinableQueue,
                 address: Optional[str, int] = None, global_mgr_address: Optional[Tuple[str, int]] = None):
        """
        :param test_q: q (with asyncio-to-mp bridge) for populating tests
        :param results_q: q (with asyncio-to-mp bridge) for gathering results
        :param global_mgr_address: address hosting the global fixtures
        """
        # for accumulating tests and exit status:
        super().__init__()
        self._pending: Dict[str, TestState] = {}
        self._exit_results: List[ResultExit] = []
        self._count = 0
        self._target_count = 0
        self._session_start_time = time.time()
        # for information output:
        self._reporter = BasicReporter()
        self._rusage = None
        # for information output:
        self._reporter = BasicReporter()
        self._rusage = None
        if global_mgr_address is not None:
            Global.Manager.singleton(address=global_mgr_address, as_client=False)
        self._test_q = AsyncMPQueue(wrapped_q=test_q)
        self._results_q = AsyncMPQueue(wrapped_q=results_q)
        self._active = False
        self._test_end_signalled = False
        self._all_workers_done = False
        self._address = address

    @classmethod
    def as_local(cls, test_q: multiprocessing.JoinableQueue, results_q: multiprocessing.JoinableQueue):
        return Orchestrator(test_q=test_q, results_q=results_q)

    @classmethod
    def as_client(cls, test_q: multiprocessing.JoinableQueue, results_q: multiprocessing.JoinableQueue,
                  address: Tuple[str, int]):
        orchestrator = Orchestrator(test_q=test_q, results_q=results_q, address=address)
        orchestrator.connect()
        return orchestrator

    @classmethod
    def as_server(cls, test_q: multiprocessing.JoinableQueue, results_q: multiprocessing.JoinableQueue,
                  address: Tuple[str, int]):
        orchestrator = Orchestrator(test_q=test_q, results_q=results_q, address=address)
        orchestrator.start()
        return orchestrator

    class MainSession:

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

        async def signal_all_workers_done(self, count: int):
            """
            signal through orchestration manager all tests are sent
            """
            for _ in range(count):
                if await self._test_q.put(None):
                    count += 1
            self._all_workers_done = True
            with suppress(Exception):
                # should already be closed, but...
                self._test_q.close()

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
                    f"Mem consumed: {unshared_mem/1000.0}M\n\n"
                )
            if self._count != self._target_count:
                self._write_sep(
                    '!', "{} tests unaccounted for {} out of {}".format(self._target_count - self._count,
                                                                        self._count, self._target_count))
            sys.stdout.flush()

        def _process_worker_message(self, hook, result: ResultType):
            """
            Process a message (as a worker) from the coordinating process

            :param hook: pytest hook to present test report
            :param result: the result to process
            """
            try:
                if isinstance(result, ResultTestStatus):
                    if result.report.when == 'call' or (result.report.when == 'setup' and not result.report.passed):
                        self._count += 1
                    hook.pytest_runtest_logreport(report=result.report)
                elif isinstance(result, ReportStarted):
                    hook.pytest_runtest_logstart(nodeid=result.nodeid, location=result.location)
                elif isinstance(result, ReportFinished):
                    hook.pytest_runtest_logfinish(nodeid=result.nodeid, location=result.location)
                elif isinstance(result, ResultExit):
                    # process is complete, so close it and set to None
                    self._exit_results.append(result)
                elif isinstance(result, ResultException):
                    raise Exception("Internal ERROR") from result
                elif result is None:
                    pass
                elif isinstance(result, TestState):
                    if result.state == TestStateEnum.STARTED:
                        self._pending[result.test_id] = result
                    elif result.state == TestStateEnum.FINISHED:
                        with suppress(Exception):
                            del self._pending[result.test_id]
                    elif result.state == TestStateEnum.RETRY:
                        with suppress(Exception):
                            del self._pending[result.test_id]
                        self._test_q.put(TestBatch(test_ids=[result.test_id]))
                else:
                    raise Exception(f"Internal Error: Unknown result type: {type(result)}!!")

            except Exception as e:
                sys.stdout.write("INTERNAL_ERROR> %s\n" % str(e))

        async def _process_worker_died(self, message: ClientDied):
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
                            await self._results_q.put([ResultTestStatus(
                                TestReport(nodeid=tid,
                                           location=("<<unknown>>", None, "<<unknown>>"),
                                           keywords={},
                                           outcome='skipped',
                                           when='call',
                                           longrepr=f"Host {test_state.host} or process on host became  "
                                                    "unresponsive or died.  Test cannot be retried as it "
                                                    " is part of a batch"
                                           ))])
                always_print(f"\nA worker {key} has died: {message.message}\n")
            else:
                # noinspection PyUnresolvedReferences
                debug_print(f"\nWorker-{key} finished\n")

        async def read_results(self, hook):
            """
            read results and take actions (in batches)
            :param hook: pytest hook needed for test reporting
            """
            exceptions = []
            try:
                test_count = 0
                while not self._all_workers_done:
                    result_batch: Union[List[ResultType], None] = await self._results_q.get()
                    if result_batch is None:
                        always_print(f">>>>>Premature ending found in results queue.<<<<<")
                        break
                    if isinstance(result_batch, ClientDied):
                        await self._process_worker_died(result_batch)
                    elif isinstance(result_batch, Exception):
                        always_print(f"Exception from remote worker: {result_batch}", as_error=True)
                        exceptions.append(result_batch)
                    elif isinstance(result_batch, ResultExit):
                        self._exit_results.append(result_batch)
                    elif isinstance(result_batch, list):
                        for result in result_batch:
                            if isinstance(result, ResultException):
                                raise Exception("Internal ERROR") from result
                            else:
                                test_count += 1
                                self._process_worker_message(hook,  result)
                    if result_batch is None:
                        break
                # process stragglers left in results queue
                try:
                    result_batch = await self._results_q.get(immediate=True)
                    while result_batch:
                        if isinstance(result_batch, list):
                            for result in result_batch:
                                self._process_worker_message(hook, result)
                        elif isinstance(result_batch, ResultExit):
                            self._exit_results.append(result_batch)
                        result_batch = await self._results_q.get(immediate=True)
                except Empty:
                    pass
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
                await self._results_q.wait()

        async def populate_test_queue(self, tests: List[TestBatch]) -> None:
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
                self.populate_test_queue(test_batches)
            )
            try:
                # only master will read results and post reports through pytest
                await self.read_results(session.config.hook)
            except ClientDied:
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
                self._active = False
                with suppress(Exception):
                    self._results_q.close()
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

        def output_summary(self):
            self._output_summary()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            self.shutdown()

        def shutdown(self):
            """
            shutdown services
            """
            with suppress(Exception):
                self._test_q.close()
            with suppress(Exception):
                self._results_q.close()
            self._pending = {}

    def conduct_session(self,
                        test_q: multiprocessing.JoinableQueue, results_q: multiprocessing.JoinableQueue):
        with self.MainSession(test_q=test_q, results_q=results_q) as session:
            session.join()

    def main(self, address: Tuple[str, int]):
        """
        main entry point called from a TestSession
        """
        orchestrator = Orchestrator.as_server(address=address)
        orchestrator.join()
