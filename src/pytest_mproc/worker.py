import os
import signal
import subprocess
import binascii
from glob import glob
from pathlib import Path

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
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from pytest_mproc.ptmproc_data import ModeEnum
from pytest_mproc.user_output import always_print
from pytest_mproc import TestError, user_output, get_ip_addr, get_auth_key
from pytest_mproc.data import (
    ClientDied,
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
from pytest_mproc.fixtures import Global
from pytest_mproc.utils import BasicReporter


"""maximum time between reporting status back to coordinator"""
MAX_REPORTING_INTERVAL = 1.0  # seconds


class WorkerSession:
    """
    Handles reporting of test status and the like
    """

    def __init__(self, worker_id: str, test_q: JoinableQueue, result_q: Queue):
        self._this_host = get_ip_addr()
        self._id = worker_id
        self._name = worker_id
        self._count = 0
        self._session_start_time = time.time()
        self._buffered_results = []
        self._buffer_size = 1
        self._timestamp = time.time()
        self._last_execution_time = time.time()
        self._resource_utilization = ResourceUtilization(-1.0, -1.0, -1.0, -1)
        self._reporter = BasicReporter()
        self._test_q = test_q
        self._result_q = result_q
        self._ip_addr = get_ip_addr()
        self._pid = os.getpid()

    def _put(self, result: Union[TestStateEnum, ResultType, ResultExit, ClientDied,
                                 ReportStarted, ReportFinished], timeout=None):
        """
        Append test result data to queue, flushing buffered results to queue at watermark level for efficiency
        :param result: test result data
        :param timeout: timeout after this many seconds, if specified
        :raises: TimeoutError is not executed in time
        """
        if isinstance(result, (ResultExit, ClientDied)):
            self._result_q.put(result)
        else:
            self._buffered_results.append(result)
        if isinstance(result, (TestStateEnum, ResultExit)) \
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

            if session.config.option.collectonly:
                return  # should never really get here, but for consistency
            for test_batch in session.items_generator:
                if test_batch is None:
                    break
                # test item comes through as a unique string nodeid of the test
                # We use the pytest-collected mapping we squirrelled away to look up the
                # actual _pytest.python.Function test callable
                # NOTE: session.items is an iterator, as wet upon construction of WorkerSession
                for test_id in test_batch.test_ids:
                    has_error = False
                    bare_test_id = test_id.split(os.sep)[-1]
                    item = session._named_items[bare_test_id]
                    self._put(TestState(TestStateEnum.STARTED, self._this_host, self._pid, test_id, test_batch))
                    try:
                        item.config.hook.pytest_runtest_protocol(item=item, nextitem=None)
                    except TestError:
                        has_error = True
                        self._put(TestState(TestStateEnum.RETRY, self._this_host, self._pid, test_id, test_batch))
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        import traceback
                        raise Exception(f"Exception running test: {e}: {traceback.format_exc()}") from e

                    finally:
                        if not has_error:
                            with suppress(Exception):
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

    @pytest.mark.tryfirst
    def pytest_collection_finish(self, session):
        """
        Invoked once pytest has collected all information about which tests to run.
        Those items are squirrelled away in a different attributes, and a generator
        is put in its place to draw from.
        :param session: the pytest session
        :return: the generator of tests to run
        """
        assert self._test_q is not None

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
        session.items_generator = generator()
        session._named_items = {item.nodeid.split(os.sep)[-1]: item for item in session.items}
        return session.items

    @classmethod
    def start(cls,
              index: Tuple[int, int],
              orchestration_port: int,
              global_mgr_port: int,
              args: List[str],
              addl_env: Dict[str, str],
              root_path: Optional[Path],
              worker_artifacts_dir: Path,
              artifacts_root: Path,
              venv_path: Optional[Path]) -> Tuple[subprocess.Popen, subprocess.Popen]:
        """
        :param index: worker index unique to host machine
        :param global_mgr_port: (forwarded) localhost port of global manager
        :param orchestration_port: (forwarded) localhost port of orchestration manager
        :param args: args to append to pytest process of worker
        :param addl_env: additional environment variables when launching worker
        :param root_path: remote path where bundle is unpacked, or cwd if None
        :param worker_artifacts_dir: RELATIVE path for storing artifacts, relative to run dir
        :param artifacts_root: root artifact path (absolute) for all workers
        :param venv_path: remote path where virtual environment is installed, ignored if None
        :return: Process created for new worker
        """
        from pytest_mproc.fixtures import Node
        always_print(f"Starting worker on ports {orchestration_port} {global_mgr_port}", as_error=True)
        Node.Manager.singleton()
        env = addl_env
        if root_path is not None:
            site_pkgs = root_path / 'site-packages'
            env['PYTHONPATH'] = str(site_pkgs)
        env.update({
            "PTMPROC_WORKER": "1",
            "AUTH_TOKEN_STDIN": "1",
            'PTMPROC_VERBOSE': '1' if user_output.is_verbose else '0',
            'PTMPROC_NODE_MGR_PORT': str(Node.Manager.PORT),
        })
        executable = str(venv_path / 'bin' / 'python3') if venv_path else sys.executable
        main_run_path = root_path / 'run' if root_path and root_path != Path(os.getcwd()) else Path(os.getcwd())
        main_run_path.mkdir(exist_ok=True, parents=True)
        run_dir = (root_path or Path(os.getcwd())) / 'workers' / f'run-Worker-{index[0]}-{index[1]}'
        run_dir.mkdir(exist_ok=True, parents=True)
        files = glob(str(main_run_path / '*'))
        for f in [Path(f) for f in files if Path(f) != main_run_path]:
            target = run_dir / f.relative_to(main_run_path)
            if not target.exists() and not str(target.name).startswith('Worker-') \
                    and not target.name == worker_artifacts_dir.name:
                target.symlink_to(f)
        # this is to create a symlink to a global path structure
        # to an artifact path relative to run_dir so that runtime sees the relative artifact path
        # dir it expects, ultimately derived from the relative artifact path in project config
        worker_artifacts_path = run_dir / worker_artifacts_dir
        artifacts_path = artifacts_root / f'Worker-{index[0]}-{index[1]}'
        artifacts_path.mkdir(exist_ok=True, parents=True)
        assert artifacts_path.is_absolute()
        if not worker_artifacts_path.exists():
            with suppress(Exception):
                worker_artifacts_path.symlink_to(artifacts_path)
        log = artifacts_path / 'pytest_run.log'
        stdout = subprocess.DEVNULL if not user_output.is_verbose else sys.stderr
        tee_proc = subprocess.Popen(['tee', log],
                                    stdout=stdout,
                                    stderr=sys.stderr,
                                    bufsize=0,
                                    stdin=subprocess.PIPE)
        cmd = f"{executable} -m {__name__} {orchestration_port} {global_mgr_port} {Node.Manager.PORT} "\
            f"Worker-{index[0]}-{index[1]} {' '.join(args)}"
        proc = subprocess.Popen(
            cmd,
            stdout=tee_proc.stdin,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            cwd=str(run_dir),
            env=env,
            bufsize=0,
            shell=True,
        )
        proc.stdin.write(binascii.b2a_hex(get_auth_key()) + b'\n')
        proc.stdin.close()
        always_print(f"Launched command '{cmd}' with pid {proc.pid}")
        return proc, tee_proc

    # noinspection SpellCheckingInspection
    def pytest_internalerror(self, __excrepr):
        try:
            self._put(ResultException(SystemError("Internal pytest error")))
        except Exception as e:
            always_print(f"{self._name} failed to pust internall error: {e}")

    # noinspection SpellCheckingInspection
    def pytest_runtest_logstart(self, nodeid, location):
        if "PTMPROC_REPORT_TEST_START" in os.environ:
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
    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_logreport(self, report):
        """
        report only status of calls (not setup or teardown), unless there was an error in those
        stash report for later end-game parsing of failures

        :param report: report to draw info from
        """
        try:
            self._put(ResultTestStatus(report))
        except Exception as e:
            always_print(f"Failed to log report to main node: {e}")

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
        self.set_singleton(None)
        return True

    @classmethod
    def set_singleton(cls, singleton):
        cls._singleton = singleton

    @classmethod
    def singleton(cls):
        return cls._singleton

    _singleton = None


def pytest_cmdline_main(config):
    config.ptmproc_worker = WorkerSession.singleton()
    config.ptmproc_config.mode = ModeEnum.MODE_WORKER


def main(orchestration_port: int, global_mgr_port: int, node_mgr_port: int, args: List[str], worker_id: str):
    from pytest_mproc import plugin  # ensures auth_key is set
    from pytest_mproc.fixtures import Node
    # noinspection PyUnresolvedReferences
    from pytest_mproc.worker import WorkerSession  # to make Python happy
    assert plugin  # to prevent flake8 unused import
    # from pytest_mproc.worker import WorkerSession
    from pytest_mproc.orchestration import OrchestrationManager
    assert node_mgr_port == Node.Manager.PORT
    Node.Manager.PORT = node_mgr_port
    Node.Manager.singleton()  # forces creation on defined port
    Global.Manager.singleton(address=('localhost', global_mgr_port), as_client=True)
    mgr = OrchestrationManager.create_client(address=('localhost', orchestration_port))
    # noinspection PyUnresolvedReferences
    mgr.register_worker((get_ip_addr(), os.getpid()))
    # noinspection PyUnresolvedReferences
    test_q = mgr.get_test_queue().raw()
    # noinspection PyUnresolvedReferences
    result_q = mgr.get_results_queue().raw()
    assert test_q is not None
    assert result_q is not None
    worker = WorkerSession(worker_id=worker_id,
                           test_q=test_q,
                           result_q=result_q,
                           )
    WorkerSession.set_singleton(worker)
    assert WorkerSession.singleton() is not None
    status = None
    # noinspection PyBroadException
    try:
        args += ['--as-worker-node', f'localhost:{global_mgr_port},{orchestration_port}']
        status = pytest.main(args)
        if isinstance(status, pytest.ExitCode):
            status = status.value
    except Exception as e:
        msg = f"Worker {get_ip_addr()}-{os.getpid()} died with exception {e}\n {traceback.format_exc()}"
        os.write(sys.stderr.fileno(), msg.encode('utf-8'))
        with suppress(Exception):  # if other end died, result_q is gone, so suppress exceptions
            result_q.put(ClientDied(os.getpid(), get_ip_addr(), errored=True, message=msg))
    finally:
        signal.alarm(10)
        with suppress(Exception):
            mgr.completed(get_ip_addr(), os.getpid())
        with suppress(Exception):
            Node.Manager.shutdown()
    return status


if __name__ == "__main__":
    sys.stdout.write(f"PID: {os.getpgid(os.getpid())}")
    _orchestration_port = int(sys.argv[1])
    _global_mgr_port = int(sys.argv[2])
    _node_mgr_port = int(sys.argv[3])
    _worker_id = sys.argv[4]
    sys.exit(main(orchestration_port=int(_orchestration_port),
                  global_mgr_port=int(_global_mgr_port),
                  node_mgr_port=int(_node_mgr_port),
                  args=sys.argv[5:],
                  worker_id=_worker_id))
