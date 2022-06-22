import asyncio
import multiprocessing
import os

import pytest
import resource
import socket
import subprocess
import sys
import tempfile
import time

from multiprocessing import Queue
from contextlib import suppress, asynccontextmanager
from multiprocessing import Semaphore
from pathlib import Path
from queue import Empty
from threading import RLock
from typing import List, Optional, Union, Dict, AsyncIterator, Iterable, AsyncGenerator, Tuple

from _pytest.reports import TestReport

from pytest_mproc import (
    DEFAULT_PRIORITY,
    FatalError,
    find_free_port,
    get_auth_key,
    resource_utilization,
    Settings, AsyncMPQueue,
)
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.data import (
    ClientDied,
    GroupTag,
    ResultExit,
    TestBatch,
    TestState,
    TestStateEnum, AllClientsCompleted, ReportStarted, ReportFinished,
)
from pytest_mproc.data import ResultException, ResultTestStatus, ResultType
from pytest_mproc.fixtures import Global
from pytest_mproc.http import HTTPSession
from pytest_mproc.orchestration import OrchestrationManager
from pytest_mproc.ptmproc_data import ProjectConfig, RemoteWorkerConfig
from pytest_mproc.remote.bundle import Bundle
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc.utils import BasicReporter

__all__ = ["Orchestrator", "RemoteSession"]

lock = RLock()
WorkerSpec = Union[List[RemoteWorkerConfig], AsyncIterator[RemoteWorkerConfig]]
POLLING_INTERVAL_HOST_VALIDATION = 1


# noinspection PyBroadException
def _localhost():
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        try:
            return socket.gethostname()
        except Exception:
            print(">>> Cannot get ip address of host.  Return 127.0.0.1 (localhost)")
            return "127.0.0.1"


class RemoteSession:
    """
    class to conduct a remote session with multiple remote worker nodes
    """

    def __init__(self,
                 project_config: ProjectConfig,
                 remote_sys_executable: Optional[str],
                 mgr: OrchestrationManager
                 ):
        """
        :param project_config: user defined project parameters needed for creating the bundle to send
        :param remote_sys_executable: optional string path to remote executable for Python to use (must version-match)
        :param mgr:
        """
        super().__init__()
        self._project_config = project_config
        self._remote_sys_executable = remote_sys_executable
        self._mp_mgr = mgr
        self._exc_manager = multiprocessing.managers.SyncManager(authkey=get_auth_key())
        self._exc_manager.register("Queue", multiprocessing.Queue, exposed=["get", "put", "empty"])
        if mgr.uri and mgr.uri.startswith('delegated://'):
            destination_text = mgr.uri.rsplit('://', maxsplit=1)[-1]
            if '@' in destination_text:
                self._username, port_text = destination_text.split('@', maxsplit=1)
            else:
                self._username = Settings.ssh_username
                port_text = destination_text
            self._delegation_port = int(port_text) if port_text else -1
        else:
            self._delegation_port = None
        self._validate_task = None
        self._session_task: Optional[asyncio.Task] = None
        self._procs = {}

    @asynccontextmanager
    async def start_workers(
            self,
            server: str, server_port: int,
            hosts_q: Queue,
            timeout: Optional[float] = None,
            deploy_timeout: Optional[float] = None,
            env: Dict[str, str] = None,
    ) -> AsyncGenerator[Tuple[int, int], Tuple[int, int]]:
        port_q = asyncio.Queue(1)
        delegation_q = asyncio.Queue(1)
        with tempfile.TemporaryDirectory() as tmp_root,\
                Bundle.create(root_dir=Path(tmp_root),
                              project_config=self._project_config,
                              system_executable=self._remote_sys_executable) as bundle:
            try:
                self._session_task = asyncio.create_task(
                    bundle.execute_remote_multi(
                        auth_key=get_auth_key(),
                        timeout=timeout,
                        deploy_timeout=deploy_timeout,
                        server_info=(server, server_port),
                        delegation_port=self._delegation_port,
                        hosts_q=hosts_q,
                        port_q=port_q,
                        delegation_q=delegation_q,
                        env=env
                    )
                )
                if self._delegation_port in (-1, ):
                    self._delegation_port = await asyncio.wait_for(port_q.get(), timeout=deploy_timeout)
                    global_mgr_port = await asyncio.wait_for(port_q.get(), timeout=deploy_timeout)
                    delegation_host = await asyncio.wait_for(delegation_q.get(), timeout=deploy_timeout)
                    yield self._delegation_port, global_mgr_port
                else:
                    delegation_host = None
                    yield -1, -1
                if delegation_host:
                    tries = 30
                    polling_interval = 1  # second
                    while tries:
                        # noinspection PyBroadException
                        try:
                            mgr = OrchestrationManager.create(uri=f"{delegation_host}:{self._delegation_port}",
                                                              as_client=True)
                            results_q = mgr.get_results_queue()
                            break
                        except Exception:
                            tries -= 1
                            always_print(f"Connecting.. tries left: {tries}")
                            if tries <= 0:
                                raise
                            await asyncio.sleep(polling_interval)
                else:
                    results_q = self._mp_mgr.get_results_queue()
                self._validate_task = asyncio.create_task(
                    self.validate_clients(results_q=results_q, remote_host_procs=self._procs)
                )
                self._procs, delegation_proc = await self._session_task
                self._procs = {}
                await asyncio.wait_for(self._validate_task, timeout=5)
                self._validate_task.cancel()
            except Exception as e:
                with suppress(Exception):
                    self._validate_task.cancel()
                import traceback
                print(traceback.format_exc())
                msg = f"!!! Exception in creating or executing bundle {e}:\n\n"
                with suppress(Exception):
                    await asyncio.wait_for(results_q.put(FatalError(msg)), timeout=timeout)
                raise
            finally:
                with suppress(Exception):
                    await bundle.cleanup()
                with suppress(Exception):
                    await asyncio.wait_for(results_q.put(AllClientsCompleted()), timeout=timeout)
                if self._validate_task:
                    self._validate_task.cancel()

    @staticmethod
    async def validate_clients(results_q: AsyncMPQueue,
                               remote_host_procs: Dict[str, asyncio.subprocess.Process]) -> None:
        """
        Continually ping workers to ensure they are alive, and drop worker if not, rescheduling any pending tests to
        that worker

        :param results_q: Used to signal a client died
        :param remote_host_procs: mapping of hosts to processes running remote shells to that host
        """
        while remote_host_procs:
            procs = remote_host_procs.copy()
            for host in procs:
                # test if worker is active
                try:
                    completed = subprocess.run(["ping", "-W",  "1", "-c", "1", host],
                                               stdout=subprocess.DEVNULL,
                                               timeout=5)
                    active = completed.returncode == 0
                except TimeoutError:
                    active = False
                # reschedule any tests in progress for tha worker if not
                if not active:
                    always_print(f"Host {host} unreachable!")
                    proc = remote_host_procs.get(host)
                    if proc.pid is None:
                        os.write(sys.stderr.fileno(), f"Process {proc.pid} died!".encode('utf-8'))
                    else:
                        with suppress(Exception):
                            proc.terminate()
                        with suppress(Exception):
                            proc.kill()
                    # this will attempt to reschedule test
                    result = ClientDied(proc.pid, host, True)
                    await results_q.put(result)
                    with lock, suppress(Exception):
                        del remote_host_procs[host]
                else:
                    if host in remote_host_procs and procs[host].returncode is not None:
                        del remote_host_procs[host]
                    debug_print(f"{host} is alive")
            time.sleep(POLLING_INTERVAL_HOST_VALIDATION)

    async def shutdown(self):
        self._procs = {}
        with suppress(Exception):
            self._validate_task.cancel()
        if self._session_task is not None:
            try:
                await asyncio.wait_for(self._session_task, timeout=5)
            except asyncio.TimeoutError:
                with suppress(Exception):
                    self._session_task.cancel()


class Orchestrator:
    """
    class that acts as Main point of orchestration
    """

    def __init__(self,
                 uri: str = f"{_localhost()}:{find_free_port()}",
                 project_config: Optional[ProjectConfig] = None,
                 ):
        """
        :param uri: uri expressing destination of multiprocess server
        :param project_config: user-defined project parameters
        """
        self._uri = uri
        dest = uri.split("://")[-1]
        if '@' in dest:
            self._username, _ = dest.split('@', maxsplit=1)[0]
        else:
            self._username = None
        self._project_config = project_config
        self._server_uri = uri

        # for accumulating tests and exit status:
        self._tests: List[TestBatch] = []  # set later
        self._pending: Dict[str, TestState] = {}
        self._exit_results: List[ResultExit] = []
        self._count = 0
        self._session_start_time = time.time()
        # definitions and  managers responsible for flow of tests and results:
        project_name = project_config.project_name if project_config else None
        self._orch_manager = OrchestrationManager.create(uri, project_name=project_name, as_client=False)
        self._finish_sem = Semaphore(0)
        if uri.startswith("delegated://"):
            self._test_q = None
            self._results_q = None
        else:
            self._test_q = self._orch_manager.get_test_queue()
            self._results_q = self._orch_manager.get_results_queue()
            if not uri.startswith('ssh://'):
                host_and_port = uri.split('://', maxsplit=1)[-1].split('@', maxsplit=1)[-1]
                host, orch_port = host_and_port.split(':')
                port = find_free_port()
                while port == orch_port:
                    port = find_free_port()
                Global.Manager.singleton((host, port), as_client=False)
        # for information output:
        self._reporter = BasicReporter()
        self._rusage = None
        # for internal handling of requests
        self._remote_session = None
        self._hosts_q = asyncio.Queue(10)
        self._workers_task: Optional[asyncio.Task] = None
        self._populate_worker_queue_task: Optional[asyncio.Task] = None
        self._coordinator: Optional[Coordinator] = None
        self._http_session: Optional[HTTPSession] = None

    @property
    def mp_mgr(self):
        return self._orch_manager

    @property
    def uri(self):
        return self._uri

    async def populate_worker_queue(self,
                                    worker_q: asyncio.Queue,
                                    worker_configs: WorkerSpec,
                                    deploy_timeout: float,
                                    finish_sem: Semaphore,
                                    delegate_q: asyncio.Queue) -> None:
        """
        Given the set of worker configurations, populate a worker queue for the orchestration to run on
        :param worker_q: where to put workers as they are received/listed
        :param worker_configs: list or async generator of worker configurations
        :param deploy_timeout: timeout if taking to long
        :param finish_sem: if released, indicates everything is finished and to give up getting/configuring more workers
        :param delegate_q: put the first worker received as the delegate server, if needed
        """
        sem = asyncio.Semaphore(0)
        try:
            if isinstance(worker_configs, Iterable):
                for index, worker_config in enumerate(worker_configs):
                    await worker_q.put(worker_config)
                    if index == 0:
                        await delegate_q.put(worker_config.remote_host)
            else:
                async def lazy_distribution():
                    index_ = 0
                    async for worker_config_ in worker_configs:
                        if isinstance(worker_config_, HTTPSession):
                            self._http_session = worker_config_
                            continue
                        if index_ == 0:
                            await delegate_q.put(worker_config_.remote_host)
                        index_ += 1
                        sem.release()
                        await worker_q.put(worker_config_)
                        if finish_sem.acquire(block=False):
                            break

                await asyncio.wait_for(lazy_distribution(), timeout=deploy_timeout)
        finally:
            await worker_q.put(None)

    def start_local(self, num_processes: int) -> Coordinator:
        self._coordinator = Coordinator()
        self._orch_manager.register_coordinator(self._coordinator)
        self._coordinator.start_workers(uri=self._uri, num_processes=num_processes)
        return self._coordinator

    async def start_remote(
            self,
            remote_workers_config: WorkerSpec,
            deploy_timeout: Optional[int] = None,
            env: Optional[Dict[str, str]] = None,
         ):
        remote_sys_executable = 'python{}.{}'.format(*sys.version_info)
        if self._project_config is None:
            raise pytest.UsageError(
                "You must supply both a project configuration and a remotes client configuration together when "
                f"requesting automated distributed test execution"
            )
        host = self._orch_manager.host
        port_q = asyncio.Queue(2)
        port = self._orch_manager.port
        delegate_q = asyncio.Queue(2)
        self._remote_session = RemoteSession(remote_sys_executable=remote_sys_executable,
                                             project_config=self._project_config,
                                             mgr=self._orch_manager
                                             )

        async def start():
            nonlocal port_q
            async with self._remote_session.start_workers(
                    server=host,
                    server_port=port,
                    deploy_timeout=deploy_timeout,
                    hosts_q=self._hosts_q,
                    env=env
            ) as (delegate_port, global_mgr_port):
                await port_q.put(delegate_port)
                await port_q.put(global_mgr_port)
        self._workers_task = asyncio.create_task(start())
        self._populate_worker_queue_task = asyncio.create_task(asyncio.wait_for(
            self.populate_worker_queue(self._hosts_q,
                                       remote_workers_config,
                                       deploy_timeout,
                                       self._finish_sem,
                                       delegate_q=delegate_q
                                       ),
            timeout=deploy_timeout
        ))
        if self._uri.startswith('delegated://'):
            await self._establish_delegate(delegate_q=delegate_q, port_q=port_q, deploy_timeout=deploy_timeout)

    async def _establish_delegate(self, delegate_q: asyncio.Queue, port_q: asyncio.Queue,
                                  deploy_timeout: Optional[float]):
        """
        Establish a delegate server
        """
        delegated_host = await asyncio.wait_for(delegate_q.get(), timeout=deploy_timeout)
        port = await asyncio.wait_for(port_q.get(), timeout=deploy_timeout)
        global_mgr_port = await asyncio.wait_for(port_q.get(), timeout=deploy_timeout)
        polling_interval = 1  # seconds
        tries = 30
        self._orch_manager = OrchestrationManager.create(
            uri=f"{delegated_host}:{port}",
            project_name=self._project_config.project_name + "HERE",
            as_client=True,
            connect=False
        )
        while tries:
            # noinspection PyBroadException
            try:
                self._orch_manager.connect()
                break
            except Exception:
                always_print(f"Connecting; tries left: {tries}")
                tries -= 1
                if tries <= 0:
                    raise SystemError(f"Failed to start delegate manager on {delegated_host}"
                                      f":{self._orch_manager.port}")
                time.sleep(polling_interval)
        Global.Manager.singleton(address=(delegated_host, global_mgr_port), as_client=True)
        self._server_uri = f"{delegated_host}:{port}"
        if '@' not in delegated_host:
            self._uri = f"{self._username}@{delegated_host}:{port}" if self._username is not None else self._uri
        else:
            self._uri = f"{delegated_host}:{port}" if self._username is not None else self._uri
        self._test_q = self._orch_manager.get_test_queue()
        self._results_q = self._orch_manager.get_results_queue()

    @property
    def host(self):
        return self._orch_manager.host

    @property
    def port(self):
        return self._orch_manager.port

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._orch_manager.finalize()
        with suppress(asyncio.TimeoutError):
            if self._workers_task:
                await asyncio.wait_for(self._workers_task, timeout=5)
        with suppress(Exception):
            self._workers_task.cancel()
        if self._populate_worker_queue_task:
            with suppress(TimeoutError):
                await asyncio.wait_for(self._populate_worker_queue_task, 3)
        with suppress(Exception):
            self._populate_worker_queue_task.cancel()
        with suppress(Exception):
            await self._hosts_q.put(None)  # may already be closed, but in case of error path, etc
        if self._remote_session:
            await self._remote_session.shutdown()

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
        sys.stdout.write("User CPU, System CPU utilization, Add'l memory during run\n")
        sys.stdout.write("---------------------------------------------------------\n")
        for exit_result in self._exit_results:
            if exit_result.test_count > 0:
                sys.stdout.write(
                    f"Process Worker-{exit_result.worker_index} executed " +
                    f"{exit_result.test_count} tests in {exit_result.resource_utilization.time_span:.2f} " +
                    f"seconds; User CPU: {exit_result.resource_utilization.user_cpu:.2f}%, " +
                    f"Sys CPU: {exit_result.resource_utilization.system_cpu:.2f}%, " +
                    f"Mem consumed (add'l from base): "
                    f"{exit_result.resource_utilization.memory_consumed / 1000.0:.2f}M\n")
            else:
                sys.stdout.write(f"Process Worker-{exit_result.worker_index} executed 0 tests\n")
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
        length = sum([len(batch.test_ids) for batch in self._tests])
        if self._count != length:
            self._write_sep('!', "{} tests unaccounted for {} out of {}".format(length - self._count,
                            self._count, length))
        sys.stdout.flush()

    def _process_worker_message(self, hook, result: ResultType):
        """
        Process a message (as a worker) from the coordinating process

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
                                # reschedule by putting back in test queue
                                await self._test_q.put(test_state.test_batch)
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
                        debug_print(f"\nWorker-{key} finished [{self._orch_manager.count()}]\n")
                    # noinspection PyUnresolvedReferences
                    if self._orch_manager.count() <= 0:
                        os.write(sys.stderr.fileno(), b"\nNo more workers;  exiting results processing\n\n")
                elif isinstance(result_batch, Exception):
                    while True:
                        try:
                            self._test_q.get_nowait()
                        except Empty:
                            break
                    self._test_q.close()
                    raise result_batch
                elif isinstance(result_batch, ResultExit):
                    self._exit_results.append(result_batch)
                elif isinstance(result_batch, list):
                    for result in result_batch:
                        if isinstance(result, ResultException):
                            raise Exception("Internal ERROR") from result
                        else:
                            test_count += 1
                            self._process_worker_message(hook,  result)
                if self._orch_manager.count() <= 0:
                    break
                result_batch = await self._results_q.get()
            # process stragglers left in results queu
            try:
                result_batch = self._results_q.get_nowait()
                while result_batch:
                    if isinstance(result_batch, list):
                        for result in result_batch:
                            self._process_worker_message(hook, result)
                    elif isinstance(result_batch, ResultExit):
                        self._exit_results.append(result_batch)
                    result_batch = self._results_q.get_nowait()
            except Empty:
                pass
        finally:
            self._results_q.close()

    async def populate_test_queue(self, tests: List[TestBatch]):
        count = 0

        for test_batch in tests:
            # Function objects in pytest are not pickle-able, so have to send string nodeid and
            # do lookup on worker side
            await self._test_q.put(test_batch)
            count += 1
            if False and (count % 20 == 0 or count >= len(tests)):
                try:
                    await self._test_q.join()
                except EOFError:
                    os.write(sys.stderr.fileno(), b"\n>>> at least one worker disconnected or died unexpectedly\n")
        self._orch_manager.signal_all_tests_sent()

    # noinspection PyProtectedMember
    def set_items(self, tests):
        """
        :param tests: the items containing the pytest hooks to the tests to be run
        """

        # noinspection PyProtectedMember
        def priority(test_) -> int:
            tag_ = getattr(test_._pyfuncitem.obj, "_pytest_group", None)
            tag_priority = tag_.priority if tag_ is not None else DEFAULT_PRIORITY
            return getattr(test_._pyfuncitem.obj, "_pytest_priority", tag_priority)

        grouped = [t for t in tests if getattr(t._pyfuncitem.obj, "_pytest_group", None)
                   or getattr(t._pyfuncitem, "_pytest_group", None)]
        self._tests = [TestBatch([t.nodeid.split(os.sep)[-1]], priority(t)) for t in tests if t not in grouped]
        groups: Dict["GroupTag", TestBatch] = {}
        for test in grouped:
            tag = test._pyfuncitem.obj._pytest_group if hasattr(test._pyfuncitem.obj, "_pytest_group") \
                else test._pyfuncitem._pytest_group
            groups.setdefault(tag, TestBatch([], priority(test))).test_ids.append(test)
        for tag, group in groups.items():
            # noinspection PyUnresolvedReferences
            groups[tag].test_ids = [test.nodeid for test in sorted(group.test_ids, key=lambda x: priority(x))]
            groups[tag].restriction = tag.restrict_to
        self._tests.extend(groups.values())
        self._tests = sorted(self._tests, key=lambda x: x.priority)

    async def run_loop(self, session):
        """
        Populate test queue and continue to process messages from worker Processes until they complete

        :param session: Pytest test session, to get session or config information
        """
        start_rusage = resource.getrusage(resource.RUSAGE_SELF)
        start_time = time.time()
        populate_tests_task = asyncio.create_task(
            self.populate_test_queue(tests=self._tests)
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
        finally:
            if self._http_session is not None:
                await self._http_session.end_session()
            with suppress(Exception):
                # should never time out since workers are done
                await asyncio.wait_for(populate_tests_task, timeout=1)
            with suppress(Exception):
                populate_tests_task.cancel()
            end_rusage = resource.getrusage(resource.RUSAGE_SELF)
            time_span = time.time() - start_time
            self._rusage = resource_utilization(time_span=time_span, start_rusage=start_rusage, end_rusage=end_rusage)
            sys.stdout.write("\r\n")

    def output_summary(self):
        self._output_summary()

    def shutdown(self):
        self._finish_sem.release()
