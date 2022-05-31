import asyncio
import multiprocessing
import os
from queue import Empty

import resource
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
# noinspection PyUnresolvedReferences
from multiprocessing import current_process
from multiprocessing import Semaphore
from pathlib import Path

from multiprocessing import JoinableQueue, Process, Queue
from threading import RLock
from typing import List, Any, Optional, Union, Dict, AsyncIterator, AsyncGenerator

import pytest
from _pytest.reports import TestReport

from pytest_mproc import resource_utilization, find_free_port, user_output, DEFAULT_PRIORITY
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
from pytest_mproc.user_output import debug_print
from pytest_mproc.utils import BasicReporter

__all__ = ["Orchestrator", "RemoteExecutionThread", "FatalError"]

lock = RLock()


class FatalError(Exception):
    """
    raised to exit pytest immediately
    """


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


# Create proxies for Queue types to be accessible from remote clients
# NOTE: multiprocessing module has a quirk/bug where passin proxies to another separate client (e.g., on
# another machine) causes the proxy to be rebuilt with a random authkey on the other side.  Unless
# we override the constructor to force an authkey value, we will hit AuthenticationError's


class RemoteExecutionThread:

    def __init__(self,
                 project_config: ProjectConfig,
                 remote_hosts_config: Union[List[RemoteHostConfig], AsyncIterator[RemoteHostConfig]],
                 remote_sys_executable: str,
                 ):
        super().__init__()
        self._project_config = project_config
        self._remote_hosts_config = remote_hosts_config
        self._remote_sys_executable = remote_sys_executable
        self._exc_manager = multiprocessing.Manager()
        self._q = self._exc_manager.Queue()
#       self._tracker = tracker

    # noinspection PyAttributeOutsideInit
    def start_workers(
            self,
            server: str, server_port: int,
            finish_sem: Semaphore,
            result_q: Queue,
            timeout: Optional[float] = None,
            deploy_timeout: Optional[float] = None,
            auth_key: Optional[bytes] = None,
    ):
        args = (
            server, server_port, self._project_config, self._remote_hosts_config,
            self._remote_sys_executable, self._q,
            finish_sem, timeout, deploy_timeout,
            auth_key, result_q, user_output.verbose,
        )
        self._proc = multiprocessing.Process(target=RemoteExecutionThread._start, args=args)
        self._finish_sem = finish_sem
        self._proc.start()

    @staticmethod
    def _determine_cli_args(server: str, server_port: int, remote_hosts_config: List[RemoteHostConfig]):
        args = list(sys.argv[1:])  # copy of
        # remove pytest_mproc cli args to pass to client (aka additional non-pytest_mproc args)
        for arg in sys.argv[1:]:
            typ = Orchestrator.ptmproc_args.get(arg)
            if not typ:
                continue
            if arg == "--cores":
                have_core_count = False
                for cfg in remote_hosts_config:
                    if "cores" in cfg.arguments:
                        have_core_count = True
                        break
                if have_core_count:
                    index = args.index("--cores")
                    args.remove("--cores")
                    if index >= 0 and index + 1 < len(args):
                        args.remove(args[index + 1])
                    elif index >= len(args):
                        raise pytest.UsageError(f"--cores specified without a value")
                continue
            if typ in (bool,) and arg in args:
                args.remove(arg)
            elif arg in args:
                index = args.index(arg)
                if index + 1 < len(args):
                    args.remove(args[index + 1])
                args.remove(arg)
        args += ["--as-worker", f"{server}:{server_port}"]
        if "--cores" not in args:
            args += ["--cores", "1"]
        return args

    @classmethod
    def _start(cls, server: str, server_port: int, project_config: ProjectConfig,
               remote_hosts_config: List[RemoteHostConfig],
               remote_sys_executable: str, q: Queue,
               finish_sem: Semaphore, timeout: Optional[float], deploy_timeout: Optional[float],
               auth_key: bytes, result_q: Queue, verbose: bool):
        user_output.set_verbose(verbose)
        try:
            with tempfile.TemporaryDirectory() as tmpdir,\
                    Bundle.create(root_dir=Path(tmpdir),
                                  project_config=project_config,
                                  system_executable=remote_sys_executable) as bundle:
                args = cls._determine_cli_args(server, server_port, remote_hosts_config)
                user_output.set_verbose(bool(os.environ.get('PTMPROC_VERBOSE')))
                task = bundle.execute_remote_multi(
                    remote_hosts_config,
                    finish_sem,
                    *args,
                    auth_key=auth_key,
                    timeout=timeout,
                    deploy_timeout=deploy_timeout,
                    username=os.environ.get('SSH_USERNAME'),
                )
                procs = asyncio.get_event_loop().run_until_complete(task)
                with suppress(FileNotFoundError, ConnectionError):
                    # normally, result_q is closed, unless there is an exception that prevents any tests from
                    # running straight off
                    result_q.join()
                    result_q.put(AllClientsCompleted(list(procs.keys())))
        except Exception as e:
            import traceback
            msg = f"!!! Exception in creating or executing bundle {e}:\n {traceback.format_exc()}"
            os.write(sys.stderr.fileno(), msg.encode('utf-8'))
            result_q.put(FatalError(msg))
            q.put(e)

    def join(self, timeout: Optional[float] = None):
        result = self._proc.join(timeout=timeout)  # does not raise Exception on timeout
        if not self._q.empty():
            e = self._q.get()
            raise Exception("Failed to execute on remote host") from e
        return result

    def terminate(self):
        self._proc.terminate()


class Orchestrator:
    """
    class that acts as Main point of orchestration
    """

    ptmproc_args: Dict[str, Any] = {}

    def __init__(self,
                 uri: str = f"{_localhost()}:{find_free_port()}",
                 deploy_timeout: Optional[int] = None,
                 remote_sys_executable: str = 'python{}.{}'.format(*sys.version_info),
                 project_config: Optional[ProjectConfig] = None,
                 remote_clients_config: Optional[Union[List[RemoteHostConfig],
                                                       AsyncGenerator[RemoteHostConfig, RemoteHostConfig]]] = None):
        """
        :param host: local host ip addr, or use local host's ip address if not specified and determinable
        :param port: local host port to use, or find random free port if unspecified
        :param remote_clients_config: configuration of remote clients on which to launch, or unspecified if none
        """
        self._tests: List[TestBatch] = []  # set later
        self._count = 0
        self._server_uri = uri
        self._exit_results: List[ResultExit] = []
        self._session_start_time = time.time()
        self._remote_processes: Dict[str, List[asyncio.subprocess.Process]] = {}
        if remote_clients_config and project_config is None:
            raise pytest.UsageError(
                "You must supply both a project configuration and a remotes client configuration together when "
                f"requesting automated distributed test execution"
            )
        # noinspection PyUnresolvedReferences
        self._mp_manager = OrchestrationManager.create(uri, as_client=False)
        host = self._mp_manager.host
        port = self._mp_manager.port
        self._test_q = self._mp_manager.get_test_queue()
        self._result_q = self._mp_manager.get_results_queue()
        self._finish_sem = Semaphore(0)
        if remote_clients_config:
            self._remote_exec_thread = RemoteExecutionThread(remote_hosts_config=remote_clients_config,
                                                             remote_sys_executable=remote_sys_executable,
                                                             project_config=project_config)
            self._remote_exec_thread.start_workers(server=host, server_port=port, finish_sem=self._finish_sem,
                                                   deploy_timeout=deploy_timeout, auth_key=current_process().authkey,
                                                   result_q=self._result_q)
        else:
            self._remote_exec_thread = None
        self._reporter = BasicReporter()
        self._is_serving_remotes = remote_clients_config is not None
        self._pending: Dict[str, TestState] = {}

    @property
    def host(self):
        return self._mp_manager.host

    @property
    def port(self):
        return self._mp_manager.port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._remote_exec_thread is not None:
            if self._remote_exec_thread.join(timeout=5) is None:
                with suppress(Exception):
                    self._remote_exec_thread.terminate()
        self._mp_manager.shutdown()

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
                raise Exception("HERE")
            elif isinstance(result, ResultException):
                hook.pytest_internalerror(excrepr=result.excrepr, excinfo=None)
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

    @staticmethod
    def validate_clients(remote_processes, result_q: Queue):
        while remote_processes:
            for host in remote_processes:
                try:
                    completed = subprocess.run(f"ping -W 1 -c 1 {host}", timeout=5)
                    active = completed.returncode == 0
                except TimeoutError:
                    active = False
                if not active:
                    os.write(sys.stderr.fileno(), f"Host {host} unreachable!".encode('utf-8'))
                    for proc in remote_processes.get(host, []):
                        if proc.pid is None:
                            os.write(sys.stderr.fileno(), f"Process {proc.pid} died!".encode('utf-8'))
                            proc.kill()
                    # this will attempt to reschedule test
                    for proc in remote_processes[host]:
                        result_q.put(ClientDied(pid=proc.pid, host=host, errored=True))
                    with lock, suppress(Exception):
                        del remote_processes[host]
            time.sleep(1)

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
                        hook.pytest_internalerror(excrepr=result.excrepr, excinfo=None)
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
                            hook.pytest_internalerror(excrepr=result.excrepr, excinfo=None)
                        else:
                            self._process_worker_message(hook,  result)
                try:
                    result_batch = self._result_q.get_nowait()
                except Empty:
                    result_batch = None
        finally:
            self._result_q.close()

    @staticmethod
    def populate_test_queue(test_q: Queue, tests: List[TestBatch], exit_dict: Dict, uri: str):
        # noinspection PyBroadException
        count = 0
        for test_batch in tests:
            # Function objects in pytest are not pickle-able, so have to send string nodeid and
            # do lookup on worker side
            test_q.put(test_batch)
            count += 1
            if count % 20 == 0 or count >= len(tests):
                try:
                    test_q.join()
                except EOFError:
                    os.write(sys.stderr.fileno(), b"\n>>> at least one worker disconnected or died unexpectedly\n")
        client = OrchestrationManager.create(uri, as_client=True)
        # noinspection PyUnresolvedReferences
        worker_count = client.count()
        assert worker_count > 0
        for index in range(worker_count):
            test_q.put(None)
        if worker_count > 0:
            test_q.join()
        exit_dict['success'] = True

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

    def run_loop(self, session):
        """
        Populate test queue and continue to process messages from worker Processes until they complete

        :param session: Pytest test session, to get session or config information
        """
        local_mgr = multiprocessing.Manager()
        exit_dict = local_mgr.dict()
        start_rusage = resource.getrusage(resource.RUSAGE_SELF)
        start_time = time.time()
        # we are the root node, so populate the tests
        populate_tests_process = Process(target=Orchestrator.populate_test_queue,
                                         args=(self._test_q, self._tests, exit_dict, self._server_uri))
        validate_clients = Process(target=Orchestrator.validate_clients, args=([], self._result_q))
        try:
            validate_clients.start()
            populate_tests_process.start()
            self.read_results(session.config.hook)  # only master will read results and post reports through pytest
        except ClientDied:
            os.write(sys.stderr.fileno(), b"\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
            os.write(sys.stderr.fileno(), b"All clients died; Possible incomplete run\n")
            os.write(sys.stderr.fileno(), b"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n")
        except FatalError:
            populate_tests_process.terminate()
            raise session.Failed(True)
        finally:
            populate_tests_process.join(timeout=1)  # should never time out since workers are done
            populate_tests_process.terminate()
            if exit_dict.get('success') is not True:
                raise Exception("Failed to get all results")
            end_rusage = resource.getrusage(resource.RUSAGE_SELF)
            time_span = time.time() - start_time
            rusage = resource_utilization(time_span=time_span, start_rusage=start_rusage, end_rusage=end_rusage)
            sys.stdout.write("\r\n")
            self._output_summary(rusage.time_span, rusage.user_cpu, rusage.system_cpu, rusage.memory_consumed)
            # noinspection PyProtectedMember
            self._finish_sem.release()
            for client in self._mp_manager._clients:
                client.join()
            self._mp_manager.shutdown()

    # noinspection PyUnusedLocal
    def shutdown(self):
        self._reporter.write("Shutting down main...")
        self._mp_manager.shutdown()
        self._reporter.write("Shut down")
