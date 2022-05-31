import os
import socket
import subprocess
import sys
import time
import traceback
from contextlib import suppress
from multiprocessing import (
    JoinableQueue,
    Queue,
    Semaphore,
)
from multiprocessing.process import current_process
from typing import Iterator, Union
import pytest
# noinspection PyProtectedMember
from _pytest.config import _prepareconfig
import resource

from pytest_mproc import resource_utilization, TestError, user_output, get_ip_addr
from pytest_mproc.data import (
    ClientDied,
    ResultExit,
    ResourceUtilization,
    TestBatch,
    TestState,
    TestStateEnum,
)
from pytest_mproc.data import ResultException, ResultTestStatus, ResultType
from pytest_mproc.ptmproc_data import (
    PytestMprocConfig,
    PytestMprocWorkerRuntime,
)
from pytest_mproc.user_output import debug_print
from pytest_mproc.utils import BasicReporter

if sys.version_info[0] < 3:
    # noinspection PyUnresolvedReferences
    from Queue import Empty
else:
    # noinspection PyUnresolvedReferences
    from queue import Empty

"""maximum time between reporting status back to coordinator"""
MAX_REPORTING_INTERVAL = 1.0  # seconds


class WorkerSession:
    """
    Handles reporting of test status and the like
    """

    def __init__(self, index, is_remote: bool, test_q: JoinableQueue, result_q: Queue):
        self._is_remote = is_remote
        self._this_host = get_ip_addr()
        self._index = index
        self._name = "worker-%d" % (index + 1)
        self._count = 0
        self._session_start_time = time.time()
        self._buffered_results = []
        self._buffer_size = 20
        self._timestamp = time.time()
        self._last_execution_time = time.time()
        self._resource_utilization = ResourceUtilization(-1.0, -1.0, -1.0, -1)
        self._reporter = BasicReporter()
        self._test_q = test_q
        self._result_q = result_q
        self._ip_addr = get_ip_addr()
        self._pid = os.getpid()

    def _put(self, result: Union[TestStateEnum, ResultType, ResultExit], timeout=None):
        """
        Append test result data to queue, flushing buffered results to queue at watermark level for efficiency

        :param result: test result data
        :param timeout: timeout after this many seconds, if specified
        :raises: TimeoutError is not executed in time
        """
        if self._is_remote and isinstance(result, ResultTestStatus):
            os.write(sys.stderr.fileno(), b'.')
        if isinstance(result, ResultExit):
            self._result_q.put(result)
        else:
            self._buffered_results.append(result)
        if isinstance(result, (TestStateEnum, ResultExit)) \
                or len(self._buffered_results) >= self._buffer_size \
                or (time.time() - self._timestamp) > MAX_REPORTING_INTERVAL:
            self._flush(timeout)

    def _flush(self, timeout=None):
        """
        fluh buffered results out to the queue.
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

        :param session:  Where the tests generator is kept
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
                    except Exception as e:
                        import traceback
                        raise Exception(f"Exception running test: {e}: {traceback.format_exc()}")

                    finally:
                        if not has_error:
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
        finally:
            self._flush()
            assert self._buffered_results == []
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

        def generator(test_q: JoinableQueue) -> Iterator[TestBatch]:
            test = test_q.get()
            while test:
                test_q.task_done()
                yield test
                test = test_q.get()
            test_q.task_done()
        session.items_generator = generator(self._test_q)
        session._named_items = {item.nodeid.split(os.sep)[-1]: item for item in session.items}
        return session.items

    @classmethod
    def start(cls, uri: str, executable: str) -> subprocess.Popen:
        """
        :param uri: uri of main server
        :param executable: which executable to run under
        :return: Process created for new worker
        """
        from pytest_mproc.fixtures import Node
        import binascii
        env = os.environ.copy()
        env["PYTEST_WORKER"] = "1"
        env["AUTH_TOKEN_STDIN"] = "1"
        if user_output.verbose:
            env['PTMPROC_VERBOSE'] = '1'
        env['PTMPROC_NODE_MGR_PORT'] = str(Node.Manager.PORT)
        stdout = sys.stdout
        stderr = sys.stderr
        executable = executable or sys.executable
        debug_print(f">>> Starting worker with args {sys.argv[1:]}")
        proc = subprocess.Popen([executable, '-m', __name__, uri] + sys.argv[1:],
                                env=env,
                                stdout=stdout, stderr=stderr, stdin=subprocess.PIPE)
        proc.stdin.write(binascii.b2a_hex(current_process().authkey) + b'\n')
        proc.stdin.flush()
        return proc

    @staticmethod
    def run(index: int, is_remote: bool, start_sem: Semaphore, fixture_sem: Semaphore,
            test_q: JoinableQueue, result_q: Queue, sync_sem: Semaphore()) -> None:
        from pytest_mproc.orchestration import OrchestrationManager
        sync_sem.release()
        start_sem.acquire()
        worker = WorkerSession(index, is_remote, test_q, result_q)
        worker._fixture_sem = fixture_sem
        args = sys.argv[1:]
        # remove coverage args, as pytest_cov handles multiprocessing already and will apply coverage to worker
        # as a proc that was launched from main thread which itself has coverage (otherwise it will attempt
        # duplicate coverage processing and file conflicts galore)
        args = [arg for arg in args if not arg.startswith("--cov=")]
        config = _prepareconfig(args, plugins=[])
        config.option.ptmproc_config = PytestMprocConfig()
        config.ptmproc_runtime = PytestMprocWorkerRuntime()
        # unregister terminal (don't want to output to stdout from worker)
        # as well as xdist (don't want to invoke any plugin hooks from another distribute testing plugin if present)
        config.pluginmanager.unregister(name="terminal")
        config.pluginmanager.register(worker, "mproc_worker")
        worker._client = OrchestrationManager.create(uri=config.option.ptmproc_config.server_uri, as_client=True)
        workerinput = {'slaveid': "worker-%d" % worker._index,
                       'workerid': "worker-%d" % worker._index,
                       'cov_master_host': socket.gethostname(),
                       'cov_slave_output': os.path.join(os.getcwd(), "worker-%d" % worker._index),
                       'cov_master_topdir': os.getcwd()
                       }
        config.slaveinput = workerinput
        config.slaveoutput = workerinput
        try:
            # and away we go....
            config.hook.pytest_cmdline_main(config=config)
        finally:
            # noinspection PyProtectedMember
            config._ensure_unconfigure()
            worker._reporter.write(f"\nWorker-{index} finished\n")

    def pytest_internalerror(self, excrepr):
        self._put(ResultException(excrepr))

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_logreport(self, report):
        """
        report only status of calls (not setup or teardown), unless there was an error in those
        stash report for later end-game parsing of failures

        :param report: report to draw info from
        """
        self._put(ResultTestStatus(report))

    # noinspection PyUnusedLocal
    def session_finish(self):
        """
        output failure information and final exit status back to coordinator

        :param exitstatus: exit status of the test suite run
        """
        self._put(ResultExit(self._index,
                             self._count,
                             0,
                             self._last_execution_time - self._session_start_time,
                             self._resource_utilization))
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
    assert WorkerSession.singleton() is not None
    config.option.worker = WorkerSession.singleton()


def main():
    from pytest_mproc import plugin  # ensures auth_key is set
    from pytest_mproc.fixtures import Node
    # noinspection PyUnresolvedReferences
    from pytest_mproc.worker import WorkerSession  # to make Python happy
    assert plugin  # to prevent flake8 unused import
    # from pytest_mproc.worker import WorkerSession
    from pytest_mproc.orchestration import OrchestrationManager
    uri = sys.argv[1]
    mgr = OrchestrationManager.create(uri=uri, as_client=True)
    # noinspection PyUnresolvedReferences
    mgr.register_worker((get_ip_addr(), os.getpid()))
    # noinspection PyUnresolvedReferences
    test_q = mgr.get_test_queue()
    # noinspection PyUnresolvedReferences
    result_q = mgr.get_results_queue()

    worker = WorkerSession(index=os.getpid(),
                           is_remote='--as-worker' in sys.argv,
                           test_q=test_q,
                           result_q=result_q,
                           )
    WorkerSession.set_singleton(worker)
    errored = False
    msg = None
    assert WorkerSession.singleton() is not None
    status = None
    # noinspection PyBroadException
    try:
        status = pytest.main(sys.argv[3:])
        if isinstance(status, pytest.ExitCode):
            status = status.value
    except Exception as e:
        errored = True
        msg = f"Worker {get_ip_addr()}-{os.getpid()} died with exception {e}\n {traceback.format_exc()}"
        os.write(sys.stderr.fileno(), msg.encode('utf-8'))
    finally:
        result_q.put(ClientDied(os.getpid(), get_ip_addr(), errored=errored, message=msg))
        with suppress(Exception):
            Node.Manager.singleton().shutdown()


if __name__ == "__main__":
    main()
