import os
import shutil
import socket
import subprocess
import sys
import time
from multiprocessing import Semaphore, JoinableQueue, Queue, Process
from multiprocessing.process import current_process
from typing import Any, Dict, Iterator, Union

import pytest
from _pytest.config import _prepareconfig, ExitCode
import resource

from pytest_mproc import resource_utilization, TestError
from pytest_mproc.data import TestBatch, ResourceUtilization, ResultException, ResultTestStatus, ResultExit, TestState, \
    ResultType, TestStateEnum, ClientDied
from pytest_mproc.ptmproc_data import PytestMprocConfig, PytestMprocWorkerRuntime
from pytest_mproc.utils import BasicReporter

if sys.version_info[0] < 3:
    # noinspection PyUnresolvedReferences
    from Queue import Empty
else:
    # noinspection PyUnresolvedReferences
    from queue import Empty

"""maximum time between reporting status back to coordinator"""
MAX_REPORTING_INTERVAL = 1.0  # seconds


def get_ip_addr():
    hostname = socket.gethostname()
    try:
        return socket.gethostbyname(hostname)
    except Exception:
        return None


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

    def _put(self, result: Union[TestStateEnum, ResultType], timeout=None):
        """
        Append test result data to queue, flushing buffered results to queue at watermark level for efficiency

        :param result: test result data
        :param timeout: timeout after this many seconds, if specified
        :raises: TimeoutError is not executed in time
        """
        if self._is_remote and isinstance(result, ResultTestStatus):
            os.write(sys.stderr.fileno(), b'.')
            #os.write(sys.stderr.fileno(), f"\nSTATUS: {result}\n".encode('utf-8'))
        self._buffered_results.append(result)
        if isinstance(result, TestStateEnum) \
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
        global my_cov
        start_time = time.time()
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        try:
            if session.testsfailed and not session.config.option.continue_on_collection_errors:
                raise session.Failed("%d errors during collection" % session.testsfailed)

            if session.config.option.collectonly:
                return  # should never really get here, but for consistency
            for test_batch in session.items_generator:
                if test_batch is None:
                    raise Exception("DONE")
                # test item comes through as a unique string nodeid of the test
                # We use the pytest-collected mapping we squirrelled away to look up the
                # actual _pytest.python.Function test callable
                # NOTE: session.items is an iterator, as wet upon construction of WorkerSession
                for test_id in test_batch.test_ids:
                    has_error = False
                    item = session._named_items[test_id]
                    self._put(TestState(TestStateEnum.STARTED, self._this_host, self._pid, test_id))
                    try:
                        item.config.hook.pytest_runtest_protocol(item=item, nextitem=None)
                    except TestError:
                        has_error = True
                        self._put(TestState(TestStateEnum.RETRY, self._this_host, self._pid, test_id))
                    finally:
                        if not has_error:
                            self._put(TestState(TestStateEnum.FINISHED, self._this_host, self._pid, test_id))
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
            print(f">>>>>>>>>>>>>>>>> GOT TEST {test}")
            while test:
                test_q.task_done()
                yield test
                test = test_q.get()
                print(f">>>>>>>>>>>>>>>>> GOT TEST {test}")
            test_q.task_done()
        print(f">>>>>>>>>>>>>>> TESTS EXHAUSTED")
        session.items_generator = generator(self._test_q)
        session._named_items = {item.nodeid.split(os.sep)[-1]: item for item in session.items}
        return session.items

    @classmethod
    def start(cls, index: int, host: str, port: int, executable: str) -> subprocess.Popen:
        """

        :param index: index of worker being created
        :param start_sem: gating semaphore used to control # of simultaneous connections (prevents deadlock)
        :param fixture_sem: gating semaphore when querying for test fixtures
        :param test_q: Queue to pull from to get next test to run
        :param result_q: Queue to place results
        :return: Process created for new worker
        """
        import binascii
        env = os.environ.copy()
        env["PYTEST_WORKER"] = "1"
        print(f">>>>>>>>>>>> LAUNCHING WORKER {index}")
        proc = subprocess.Popen([executable,  '-m', __name__, str(index), host, str(port)] + sys.argv[1:], env=env,
                                stdout=sys.stdout.fileno(), stderr=sys.stderr.fileno(), stdin=subprocess.PIPE)
        proc.stdin.write(binascii.b2a_hex(current_process().authkey) + b'\n')
        proc.stdin.flush()
        return proc

    @staticmethod
    def run(index: int, is_remote: bool, start_sem: Semaphore, fixture_sem: Semaphore,
            test_q: JoinableQueue, result_q: Queue, sync_sem: Semaphore()) -> None:
        sync_sem.release()
        start_sem.acquire()
        worker = WorkerSession(index, is_remote, test_q, result_q)
        worker._fixture_sem = fixture_sem
        args = sys.argv[1:]
        # remove coverage args, as pytest_cov handles multiprocessing already and will applyt coverage to worker
        # as a proc that was launched from main thread which itsalef has coverage (otherwise it will attempt
        # duplicate coverage processing and file conflicts galore)
        args = [arg for arg in args if not arg.startswith("--cov=")]
        config = _prepareconfig(args, plugins=[])
        config.option.ptmproc_config = PytestMprocConfig()
        config.ptmproc_runtime = PytestMprocWorkerRuntime()
        # unregister terminal (don't want to output to stdout from worker)
        # as well as xdist (don't want to invoke any plugin hooks from another distribute testing plugin if present)
        config.pluginmanager.unregister(name="terminal")
        config.pluginmanager.register(worker, "mproc_worker")
        config.ptmproc_runtime.worker = worker
        from pytest_mproc.main import Orchestrator
        worker._client = Orchestrator.Manager(addr=(config.option.ptmproc_config.server_host,
                                                    config.option.ptmproc_config.server_port))
        workerinput = {'slaveid': "worker-%d" % worker._index,
                       'workerid': "worker-%d" % worker._index,
                       'cov_master_host': socket.gethostname(),
                       'cov_slave_output': os.path.join(os.getcwd(),
                                                         "worker-%d" % worker._index),
                       'cov_master_topdir': os.getcwd()
                       }
        config.slaveinput = workerinput
        config.slaveoutput = workerinput
        try:
            # and away we go....
            config.hook.pytest_cmdline_main(config=config)
        finally:
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

    @pytest.hookimpl(tryfirst=True)
    def pysessionfinish(self, exitstatus):
        """
        output failure information and final exit status back to coordinator

        :param exitstatus: exit status of the test suite run
        """
        self._flush()
        self._put(ResultExit(self._index,
                             self._count,
                             0,
                             self._last_execution_time - self._session_start_time,
                             self._resource_utilization))
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
    from pytest_mproc.worker import WorkerSession
    from pytest_mproc.main import Orchestrator
    from pytest_mproc.fixtures import Node
    index, host, port = sys.argv[1:4]
    index = int(index)
    port = int(port)
    mgr = Orchestrator.Manager(addr=(host, port))
    mgr.connect()
    mgr.register_worker((get_ip_addr(), os.getpid()), index)
    test_q = mgr.get_test_queue()
    result_q = mgr.get_results_queue()

    worker = WorkerSession(index=index,
                           is_remote='--as-client' in sys.argv,
                           test_q=test_q,
                           result_q=result_q,
                           )
    WorkerSession.set_singleton(worker)
    assert WorkerSession.singleton() is not None
    try:
        pytest.main(sys.argv[4:])
    except Exception as e:
        import traceback
        result_q.put(ClientDied(index, get_ip_addr(), errored=True))
    finally:
        result_q.put(ClientDied(index, get_ip_addr()))
        Node.Manager.singleton().shutdown()
    time.sleep(2)
    os.kill(os.getpid(), 9)


if __name__ == "__main__":
    main()
