import asyncio
import multiprocessing
import os
import pytest
import resource
import signal
import socket
import subprocess
import sys
import tempfile
import time

from multiprocessing import Queue
from contextlib import suppress, asynccontextmanager
from multiprocessing import Semaphore
from pathlib import Path
from queue import Empty, Full
from threading import RLock
from typing import List, Optional, Union, Dict, AsyncIterator, Iterable

from _pytest.reports import TestReport

from pytest_mproc import resource_utilization, find_free_port, user_output, DEFAULT_PRIORITY, get_auth_key, FatalError, \
    Constants, Settings
from pytest_mproc.data import (
    ClientDied,
    GroupTag,
    ResultExit,
    TestBatch,
    TestState,
    TestStateEnum, AllClientsCompleted,
)
from pytest_mproc.data import ResultException, ResultTestStatus, ResultType
from pytest_mproc.orchestration import OrchestrationManager
from pytest_mproc.ptmproc_data import ProjectConfig, RemoteHostConfig
from pytest_mproc.remote.bundle import Bundle
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc.utils import BasicReporter

__all__ = ["Orchestrator", "RemoteSession"]

lock = RLock()
WorkerSpec = Union[List[RemoteHostConfig], AsyncIterator[RemoteHostConfig]]

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

    def __init__(self,
                 project_config: ProjectConfig,
                 remote_sys_executable: Optional[str],
                 uri: str,
                 mgr: OrchestrationManager
                 ):
        """

        :param project_config: user defined project parameters needed for creaint the bundle to send
        :param remote_sys_executable: optional string path to remote executable for Python to use (must version-match)
        :param uri: uri of main server that manages test and reslt queue, etc.
        :param mgr:
        """
        super().__init__()
        self._project_config = project_config
        self._remote_sys_executable = remote_sys_executable
        self._exc_manager = multiprocessing.managers.SyncManager(authkey=get_auth_key())
        self._exc_manager.register("Queue", multiprocessing.Queue, exposed=["get", "put", "empty"])
        if uri and uri.startswith('delegated://'):
            destination_text = uri.rsplit('://', maxsplit=1)[-1]
            if '@' in destination_text:
                self._username, port_text = destination_text.split('@', maxsplit=1)
            else:
                self._username = Settings.ssh_username
                port_text = destination_text
            assert self._username
            self._delegation_port = int(port_text) if port_text else find_free_port()
        else:
            self._delegation_port = None
        self._validate_task = None
        self._result_q = mgr.get_results_queue()
        self._finish_sem = mgr.get_finalize_sem()
        self._session_task: Optional[asyncio.Task] = None

    @asynccontextmanager
    async def start_workers(
            self,
            server: str, server_port: int,
            hosts_q: Queue,
            timeout: Optional[float] = None,
            deploy_timeout: Optional[float] = None,
            auth_key: Optional[bytes] = None,
    ):
        port_q = asyncio.Queue(1)
        try:
            with Bundle.create(root_dir=Settings.tmp_root,
                               project_config=self._project_config,
                               system_executable=self._remote_sys_executable) as bundle:
                self._session_task = await asyncio.create_task(
                    bundle.execute_remote_multi(
                        auth_key=auth_key,
                        timeout=timeout,
                        deploy_timeout=deploy_timeout,
                        server_info=(server, server_port),
                        delegation_port=self._delegation_port,
                        hosts_q=hosts_q,
                        port_q=port_q,
                    )
                )
                if self._delegation_port is not None:
                    yield await asyncio.wait_for(port_q.get(), timeout=deploy_timeout)
                procs, delegation_host, delegation_proc = await self._session_task
                self._validate_task = asyncio.create_task(
                    self.validate_clients(result_q=self._result_q,
                                          remote_host_procs=procs)
                )
                with suppress(FileNotFoundError, ConnectionError):
                    # normally, result_q is closed, unless there is an exception that prevents any tests from
                    # running straight off
                    self._result_q.join()
                    self._result_q.put(AllClientsCompleted(), timeout=timeout)
        except Exception as e:
            msg = f"!!! Exception in creating or executing bundle {e}:\n\n"
            with suppress(Exception):
                self._result_q.put(FatalError(msg), timeout=timeout)
            raise
        finally:
            await self.shutdown()

    @staticmethod
    async def validate_clients(result_q: Queue, remote_host_procs: Dict[str, List[asyncio.subprocess.Process]]):
        """
        Continually ping workers to nsure they are alive, and dtop worker if not, rescheduling any pending teests to
        that worker

        :param result_q: Used to signal a client died
        :param remote_host_procs: mapping of hosts to processes running remote shells to that host
        :return:
        """
        while remote_host_procs:
            for host in remote_host_procs:
                # test if worker is active
                try:
                    completed = subprocess.run(f"ping -W 1 -c 1 {host}", timeout=5)
                    active = completed.returncode == 0
                except TimeoutError:
                    active = False
                # reschedule any tests in progress for tha worker if not
                if not active:
                    always_print(f"Host {host} unreachable!")
                    for proc in remote_host_procs.get(host, []):
                        if proc.pid is None:
                            os.write(sys.stderr.fileno(), f"Process {proc.pid} died!".encode('utf-8'))
                            proc.kill()
                    # this will attempt to reschedule test
                    for proc in remote_host_procs[host]:
                        result_q.put(ClientDied(pid=proc.pid, host=host, errored=True))
                    with lock, suppress(Exception):
                        del remote_host_procs[host]
            time.sleep(1)

    async def shutdown(self):
        self._finish_sem.release()
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
        self._project_config = project_config
        self._server_uri = uri

        # for accumulating tests and exit status:
        self._tests: List[TestBatch] = []  # set later
        self._pending: Dict[str, TestState] = {}
        self._exit_results: List[ResultExit] = []
        self._count = 0

        self._session_start_time = time.time()

        self._populate_proc = None
        self._workers_proc = None

        # definitions and  managers responsible for flow of tests and results:
        self._sm = None
        project_name = project_config.project_name if project_config else None
        self._mp_manager = OrchestrationManager.create(uri, project_name=project_name, as_client=False)
        self._finish_sem = Semaphore(0)
        self._test_q = self._mp_manager.get_test_queue()
        self._result_q = self._mp_manager.get_results_queue()


        self._reporter = BasicReporter()
        self._remote_exec_thread = None
        self._hosts_q = asyncio.Queue(10)

    async def populate_worker_queue(self, worker_q: Queue, remote_hosts_config, deploy_timeout: float, finish_sem: Semaphore,
                                    delegation_q: Queue):
        sem = asyncio.Semaphore(0)
        try:
            if isinstance(remote_hosts_config, Iterable):
                for index, worker_config in enumerate(remote_hosts_config):
                    worker_q.put(worker_config)
                    if index == 0:
                        delegation_q.put(worker_config.remote_host)
            else:
                async def lazy_distribution():
                    index = 0
                    async for worker_config in remote_hosts_config:
                        if index == 0:
                            delegation_q.put(worker_config.remote_host)
                        index += 1
                        sem.release()
                        worker_q.put(worker_config)
                        if finish_sem.acquire(block=False):
                            break

                async def timeout():
                    # if we cannot find a single client in time:
                    await asyncio.wait_for(sem.acquire(), timeout=deploy_timeout)

                await asyncio.wait_for([lazy_distribution(), timeout()], timeout=None)
        finally:
            worker_q.put(None)

    async def setup_remote(
            self, remote_workers_config: WorkerSpec,
            deploy_timeout: Optional[int] = None,
            remote_sys_executable: str = 'python{}.{}'.format(*sys.version_info),
         ):
        if self._project_config is None:
            raise pytest.UsageError(
                "You must supply both a project configuration and a remotes client configuration together when "
                f"requesting automated distributed test execution"
            )
        host = self._mp_manager.host
        port_q = asyncio.Queue(2)
        port = self._mp_manager.port
        delegate_q = self._sm.Queue()
        self._remote_exec_thread = RemoteSession(remote_hosts_config=remote_workers_config,
                                                 remote_sys_executable=remote_sys_executable,
                                                 project_config=self._project_config,
                                                 uri=self._uri,
                                                 mgr=self._mp_manager
                                                 )
        self._workers_task = await asyncio.create_task(
            self._remote_exec_thread.start_workers(
                server=host, server_port=port,
                deploy_timeout=deploy_timeout, auth_key=get_auth_key(),
                hosts_q=self._hosts_q, port_q=port_q
            )
        )
        self._populate_worker_queue_task = await asyncio.wait_for(
            self.populate_worker_queue(self._hosts_q,
                                       remote_workers_config,
                                       deploy_timeout,
                                       self._finish_sem,
                                       delegate_q)
        )
        if self._uri.startswith('delegated://'):
            delegated_host = delegate_q.get(timeout=deploy_timeout)
            port = await asyncio.wait_for(port_q.get(), timeout=deploy_timeout)
            self._mp_manager._port = port
            self._server_uri = f"{delegated_host}:{self._mp_manager.port}"
            tries = 30
            while tries:
                try:
                    if self._workers_proc.join(timeout=0):
                        raise SystemError("workers processes died unexpectedly")
                    self._mp_manager.connect(delegated_host)
                    break
                except:
                    always_print(f"Connecting; tries left: {tries}")
                    delegated_host = None
                    tries -= 1
                    if tries <= 0:
                        raise SystemError(f"Failed to start delegate manager on {delegated_host}:{self._mp_manager.port}")
                    time.sleep(1)

    @property
    def host(self):
        return self._mp_manager.host

    @property
    def port(self):
        return self._mp_manager.port

    async def __aenter__(self):
        self._tmp_dir_context = tempfile.TemporaryDirectory()
        self._tmp_dir = await self._tmp_dir_context.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        with suppress(Exception):
            await self._hosts_q.put(None)  # may already be closed, but in case of error path, etc
        if self._workers_proc is not None:
            with suppress(Exception):
                self._workers_proc.shutdown()
        if self._remote_exec_thread is not None:
            if self._remote_exec_thread.join(timeout=5) is None:
                with suppress(Exception):
                    self._remote_exec_thread.shutdown()
        with suppress(Exception):
            self._mp_manager.shutdown()
        if self._populate_proc is not None:
            self._populate_proc.shutdown()
        await self._tmp_dir_context.__aexit__()

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

    def _output_summary(self, time_span: float, ucpu: float, scpu: float, unshared_mem: float):
        """
        Output the summary of test execution
        """
        self._write_sep('=', "STATS")
        sys.stdout.write("User CPU, System CPU utilization, Add'l memory during run\n")
        sys.stdout.write("---------------------------------------------------------\n")
        for exit_result in self._exit_results:
            if exit_result.test_count > 0:
                sys.stdout.write(
                    f"Process Worker-{exit_result.worker_index} executed " +
                    f"{exit_result.test_count} tests in {exit_result.resource_utilization.time_span:.2f} " +
                    f"seconds; User CPU: {exit_result.resource_utilization.user_cpu:.2f}%, " +
                    f"Sys CPU: {exit_result.resource_utilization.system_cpu:.2f}%, " +
                    f"Mem consumed (add'l from base): {exit_result.resource_utilization.memory_consumed/1000.0:.2f}M\n")
            else:
                sys.stdout.write(f"Process Worker-{exit_result.worker_index} executed 0 tests\n")
        sys.stdout.write("\n")
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
                if result.report.when == 'call':
                    self._count += 1
                hook.pytest_runtest_logreport(report=result.report)
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
            import traceback
            traceback.print_exc()
            sys.stdout.write("INTERNAL_ERROR> %s\n" % str(e))


    def read_results(self, hook):
        try:
            test_count = 0
            error_count = 0
            result_batch: Union[List[ResultType], None] = self._result_q.get()
            while result_batch is not None:
                if isinstance(result_batch, AllClientsCompleted):
                    self._test_q.close()
                    break
                elif isinstance(result_batch, ClientDied):
                    key = f"{result_batch.host}-{result_batch.pid}"
                    if result_batch.errored:
                        for test_id, test_state in self._pending.copy().items():
                            if test_state.host != result_batch.host:
                                continue
                            del self._pending[test_id]
                            if len(test_state.test_batch.test_ids) > 1:
                                # reschedule by putting back in test queue
                                self._test_q.put(TestBatch([test_state.test_id]))
                            else:
                                os.write(sys.stderr.fileno(),
                                         f"Skipping test '{test_state.test_id}' due to client fault".encode('utf-8'))
                                self._result_q.put([ResultTestStatus(
                                    TestReport(nodeid=test_state.test_id,
                                               location=("<<unknown>>", None, "<<unknown>>"),
                                               keywords={},
                                               outcome='skipped',
                                               when='call',
                                               longrepr=f"Host {test_state.host} or process on host became  "
                                                        "unresponsive or died.  Test cannot be retried as it "
                                                        " is part of a batch"
                                               ))])
                        os.write(sys.stderr.fileno(),
                                 f"\nA worker {key} has died: {result_batch.message}\n".encode('utf-8'))
                        error_count += 1
                    else:
                        # noinspection PyUnresolvedReferences
                        debug_print(f"\nWorker-{key} finished [{self._mp_manager.count()}]\n")
                    # noinspection PyUnresolvedReferences
                    self._mp_manager.completed(result_batch.host, result_batch.pid)
                    # noinspection PyUnresolvedReferences
                    if self._mp_manager.count() <= 0:
                        os.write(sys.stderr.fileno(), b"No more workers;  exiting results processing")
                        if error_count > 0:
                            raise ClientDied(-1, "distributed-hosts", errored=True)
                        result_batch = None
                    else:
                        result_batch = self._result_q.get()
                    continue
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
                    result_batch = self._result_q.get()
                    continue
                for result in result_batch:
                    test_count += 1
                    if isinstance(result, ResultException):
                        raise Exception("Internal ERROR") from result
                    else:
                        self._process_worker_message(hook,  result)
                result_batch = self._result_q.get()
            try:
                result_batch = self._result_q.get_nowait()
            except Empty:
                result_batch = None
            while result_batch:
                if isinstance(result_batch, list):
                    for result in result_batch:
                        test_count += 1
                        if isinstance(result, ResultException):
                            raise Exception("Internal ERROR") from result
                        else:
                            self._process_worker_message(hook,  result)
                try:
                    result_batch = self._result_q.get_nowait()
                except Empty:
                    result_batch = None
        finally:
            self._result_q.close()

    async def populate_test_queue(self, test_q: Queue, tests: List[TestBatch], uri: str):
        # noinspection PyBroadException
        count = 0

        async def put(item):
            while True:
                try:
                    test_q.put_nowait(item)
                    break
                except Full:
                    await asyncio.sleep(0)  # yield

        for test_batch in tests:
            # Function objects in pytest are not pickle-able, so have to send string nodeid and
            # do lookup on worker side
            await put(test_batch)
            count += 1
            if count % 20 == 0 or count >= len(tests):
                try:
                    test_q.join()
                except EOFError:
                    os.write(sys.stderr.fileno(), b"\n>>> at least one worker disconnected or died unexpectedly\n")
        client = OrchestrationManager.create(uri, as_client=True)
        # noinspection PyUnresolvedReferences
        worker_count = client.count()
        for index in range(worker_count):
            await put(None)
        if worker_count > 0:
            test_q.join()

    # noinspection PyProtectedMember
    def set_items(self, tests):
        """
        :param tests: the items containing the pytest hooks to the tests to be run
        """

        # noinspection PyProtectedMember
        def priority(test) -> int:
            tag = getattr(test._pyfuncitem.obj, "_pytest_group", None)
            tag_priority = tag.priority if tag is not None else DEFAULT_PRIORITY
            return getattr(test._pyfuncitem.obj, "_pytest_priority", tag_priority)

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
        with tempfile.TemporaryDirectory() as tmpdir:
            self._tmpdir = tmpdir
            start_rusage = resource.getrusage(resource.RUSAGE_SELF)
            start_time = time.time()
            populate_tests_task = asyncio.create_task(
                self.populate_test_queue(test_q=self._test_q,
                                         tests=self._tests,
                                         uri=self._server_uri)
            )
            validate_clients_task = asyncio.create_task(self.validate_clients(, self._result_q))
            try:
                await self.read_results(session.config.hook)  # only master will read results and post reports through pytest
            except ClientDied:
                os.write(sys.stderr.fileno(), b"\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
                os.write(sys.stderr.fileno(), b"All clients died; Possible incomplete run\n")
                os.write(sys.stderr.fileno(), b"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n")
            except FatalError:
                raise session.Failed(True)
            finally:
                with suppress(Exception):
                    finish_sem = self._mp_manager.get_finalize_sem()
                    finish_sem.release()
                if self._sm is not None:
                    with suppress(Exception):
                        self._sm.shutdown()
                with suppress(Exception):
                    await asyncio.wait_for(populate_tests_task, timeout=1)  # should never time out since workers are done
                with suppress(Exception):
                    populate_tests_task.cancel()
                end_rusage = resource.getrusage(resource.RUSAGE_SELF)
                time_span = time.time() - start_time
                rusage = resource_utilization(time_span=time_span, start_rusage=start_rusage, end_rusage=end_rusage)
                sys.stdout.write("\r\n")
                self._output_summary(rusage.time_span, rusage.user_cpu, rusage.system_cpu, rusage.memory_consumed)
                # noinspection PyProtectedMember
                self._mp_manager.join()

    def shutdown(self):
        self._finish_sem.release()
