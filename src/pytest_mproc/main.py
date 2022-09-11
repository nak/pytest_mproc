import asyncio
import os
from abc import ABC

import pytest
import resource
import sys
import time

from contextlib import suppress
from queue import Empty
from typing import List, Optional, Union, Dict, AsyncIterator, Tuple, Any

from _pytest.reports import TestReport

from pytest_mproc import (
    FatalError,
    find_free_port)
from pytest_mproc.constants import DEFAULT_PRIORITY
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.data import (
    ClientDied,
    GroupTag,
    ResultExit,
    TestBatch,
    TestState,
    TestStateEnum, AllClientsCompleted, ReportStarted, ReportFinished, resource_utilization,
)
from pytest_mproc.data import ResultException, ResultTestStatus, ResultType
from pytest_mproc.fixtures import Global
from pytest_mproc.orchestration import OrchestrationManager
from pytest_mproc.ptmproc_data import ProjectConfig, RemoteWorkerConfig, _determine_cli_args
from pytest_mproc.remote_sessions import RemoteSessionManager
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc.utils import BasicReporter


__all__ = ["Orchestrator", "LocalOrchestrator", "RemoteOrchestrator"]


class Orchestrator(ABC):
    """
    class that acts as Main point of orchestration
    """

    def __init__(self, project_config: Optional[ProjectConfig] = None,
                 global_mgr_port: Optional[int] = None,
                 orchestration_port: Optional[int] = None,
                 ):
        """
        :param project_config: user-defined project parameters
        """
        self._project_config = project_config
        # for accumulating tests and exit status:
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
        global_mgr_address = ('localhost', global_mgr_port or find_free_port())
        self._global_mgr = Global.Manager.singleton(address=global_mgr_address, as_client=False)
        orchestration_address = ('localhost', orchestration_port or find_free_port())
        self._orchestration_mgr = OrchestrationManager.create_server(orchestration_address)
        self._test_q = self._orchestration_mgr.get_test_queue()
        self._results_q = self._orchestration_mgr.get_results_queue()
        self._active = False
        self._test_end_signalled = False

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

    async def signal_all_tests_sent(self, max_tries: Optional[float] = None):
        """
        signal through orchestration manager all tests are sent
        """
        self._orchestration_mgr.signal_all_tests_sent()
        count = 0
        target = self._orchestration_mgr.count()
        for _ in range(target):
            if await self._test_q.put(None, max_tries=max_tries):
                count += 1
        if count != target:
            always_print(f"!!! Failed to signal {target - count} workers tests are complete !!!", as_error=True)
        else:
            self._test_end_signalled = True

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

    async def read_results(self, hook):
        """
        read results and take actions (in batches)
        :param hook: pytest hook needed for test reporting
        """
        try:
            test_count = 0
            error_count = 0
            result_batch: Union[List[ResultType], None] = await self._results_q.get()
            while result_batch is not None:
                if isinstance(result_batch, AllClientsCompleted):
                    pass
                elif isinstance(result_batch, ClientDied):
                    key = f"{result_batch.host}-{result_batch.pid}"
                    # noinspection PyUnresolvedReferences
                    # self._mp_manager.completed(result_batch.host, result_batch.pid)
                    if result_batch.errored:
                        for test_id, test_state in self._pending.copy().items():
                            if test_state.host != result_batch.host:
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
                                error_count += 1
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
                        always_print(f"\nA worker {key} has died: {result_batch.message}\n")
                    else:
                        # noinspection PyUnresolvedReferences
                        debug_print(f"\nWorker-{key} finished [{self._orchestration_mgr.count()}]\n")
                    # noinspection PyUnresolvedReferences
                    if self.worker_count() <= 0:
                        os.write(sys.stderr.fileno(), b"\nNo more workers;  exiting results processing\n\n")
                elif isinstance(result_batch, Exception):
                    always_print(f"Exception from remote worker: {result_batch}", as_error=True)
                    raise
                elif isinstance(result_batch, ResultExit):
                    self._exit_results.append(result_batch)
                elif isinstance(result_batch, list):
                    for result in result_batch:
                        if isinstance(result, ResultException):
                            raise Exception("Internal ERROR") from result
                        else:
                            test_count += 1
                            self._process_worker_message(hook,  result)
                result_batch = None
                if self.worker_count() <= 0 or self._count > self._target_count:
                    break
                while result_batch is None and self.worker_count() > 0:
                    result_batch = await self._results_q.get(max_tries=10)
                if result_batch is None:
                    break
            # process stragglers left in results queue
            try:
                result_batch = await self._results_q.get(max_tries=6)
                while result_batch:
                    if isinstance(result_batch, list):
                        for result in result_batch:
                            self._process_worker_message(hook, result)
                    elif isinstance(result_batch, ResultExit):
                        self._exit_results.append(result_batch)
                    result_batch = await self._results_q.get(max_tries=6)
            except Empty:
                pass
        except KeyboardInterrupt:
            always_print("\n!!! Testing interrupted reading of results !!!")
            raise
        except Exception as e:
            always_print(f"Exception during results processing, shutting down: {e}", as_error=True)
        finally:
            await self.signal_all_tests_sent()

    async def populate_test_queue(self, tests: List[TestBatch]) -> None:
        """
        Populate test queue (asynchronously) given the set of tests
        :param tests: complete set of tests
        """
        always_print("POPULATING TESTS")
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
        except Exception as e:
            always_print(f"!!! Exception while populating tests; shutting down: {e} !!!")
        finally:
            await self.signal_all_tests_sent()

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
            self._test_q.close()
            self.shutdown()
        finally:
            self._active = False
            with suppress(Exception):
                self._results_q.close()
            with suppress(Exception):
                # should never time out since workers are done
                await asyncio.wait_for(populate_tests_task, timeout=1)
            with suppress(Exception):
                if not self._test_end_signalled:
                    await self.signal_all_tests_sent(max_tries=3)
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

    def worker_count(self) -> int:
        return self._orchestration_mgr.count()

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


class LocalOrchestrator(Orchestrator):
    """
    class that acts as Main point of orchestration
    """

    def __init__(self, project_config: Optional[ProjectConfig] = None):
        """
        :param project_config: user-defined project parameters
        """
        super().__init__(project_config)
        # noinspection PyUnresolvedReferences
        self._coordinator = Coordinator(
            coordinator_index=1,
            global_mgr_port=self._global_mgr.port,
            orchestration_port=self._orchestration_mgr.port,
            artifacts_dir=project_config.artifacts_dir if project_config else "artifacts"
        )
        # self._orchestration_mgr.register_coordinator(host='localhost', coordinator=self._coordinator)

    def start(self, num_processes: int, env: Optional[Dict[str, str]] = None) -> Coordinator:
        # one core for each worker times num_processes workers
        args, addl_env = _determine_cli_args(worker_config=None)
        addl_env.update(env or {})
        args = [str(arg) for arg in args]
        self._coordinator.start_workers(num_processes=num_processes, addl_env=addl_env, args=args)
        return self._coordinator

    def shutdown(self):
        try:
            self._coordinator.shutdown(timeout=10)
        except Exception as e:
            always_print(f"!!! coordinator failed to shutdown properly !!: {e}")
        super().shutdown()


class RemoteOrchestrator(Orchestrator):
    """
    class that acts as Main point of orchestration locally to remote managers/workers
    """

    def __init__(self,
                 project_config: Optional[ProjectConfig] = None,
                 global_mgr_port: Optional[int] = None,
                 orchestration_port: Optional[int] = None,
                 ):
        """
        :param project_config: user-defined project parameters
        """
        super().__init__(project_config, global_mgr_port=global_mgr_port, orchestration_port=orchestration_port)
        remote_sys_executable = 'python{}.{}'.format(*sys.version_info)
        self._remote_session = RemoteSessionManager(remote_sys_executable=remote_sys_executable,
                                                    project_config=self._project_config,
                                                    )

    async def start(
            self,
            remote_workers_config: AsyncIterator[RemoteWorkerConfig],
            num_cores: int,
            env: Optional[Dict[str, str]] = None,
         ):
        if self._project_config is None:
            raise pytest.UsageError(
                "You must supply both a project configuration and a remotes client configuration together when "
                f"requesting automated distributed test execution"
            )
        always_print("Gathering remote workers...")
        num_cores = num_cores or 1
        tasks = []
        try:
            async for config in remote_workers_config:
                always_print(f"Got remote worker {config.remote_host}")
                args, addl_env = _determine_cli_args(worker_config=config)
                addl_env.update(env or {})
                # noinspection PyTypeChecker
                tasks += [
                    asyncio.create_task(self._remote_session.start_worker(
                        artifacts_dir=self._project_config.artifacts_path,
                        worker_config=config,
                        coordinator_q=self._orchestration_mgr.get_coordinator_q(config.remote_host),
                        results_q=self._orchestration_mgr.get_results_queue(),
                        args=args,
                        local_global_mgr_port=self._global_mgr.port,
                        local_orchestration_port=self._orchestration_mgr.port,
                        env=addl_env,
                    )) for _ in range(num_cores)
                ]
                always_print(f"Created worker(s) for {config.remote_host}")
        except Exception as e:
            import traceback
            always_print(traceback.format_exc(), as_error=True)
            always_print(f"Failed to start a worker, aborting: {e}")
            self.shutdown()
            raise RuntimeError(f"Failed to start worker, aborting: {e}") from e
        excs = []
        worker_count = 0
        completed, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        for task in completed:
            try:
                result = task.result()
                if isinstance(result, Exception):
                    excs.append(result)
                else:
                    worker_count += 1
            except Exception as e:
                excs.append(e)
        if worker_count == 0:
            raise SystemError("All workers failed to start")

    @property
    def host(self):
        return self._orchestration_mgr.host

    @property
    def port(self):
        return self._orchestration_mgr.port

    def shutdown(self, normally: bool = False):
        self._remote_session.shutdown()
        super().shutdown()
