import os
import subprocess
import sys
import time
import traceback
from contextlib import suppress
from multiprocessing import (
    JoinableQueue,
    Queue,
)
from typing import Iterator, Union

import binascii
import pytest
# noinspection PyProtectedMember
import resource

from pytest_mproc import resource_utilization, TestError, user_output, get_ip_addr, get_auth_key
from pytest_mproc.data import (
    ClientDied,
    ResultExit,
    ResourceUtilization,
    TestBatch,
    TestState,
    TestStateEnum, ReportStarted, ReportFinished,
)
from pytest_mproc.data import ResultException, ResultTestStatus, ResultType
from pytest_mproc.fixtures import Global
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
        self._name = "worker-%d" % index
        self._count = 0
        self._session_start_time = time.time()
        self._buffered_results = []
        self._buffer_size = 5
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
                        self.pytest_runtest_logstart(item.nodeid, item.location)
                        item.config.hook.pytest_runtest_protocol(item=item, nextitem=None)
                        self.pytest_runtest_logfinish(item.nodeid, item.location)
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

        def generator() -> Iterator[TestBatch]:
            test = self._test_q.get()
            while test:
                self._test_q.task_done()
                yield test
                test = self._test_q.get()
            self._test_q.task_done()
        session.items_generator = generator()
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
        env = os.environ.copy()
        env.update({
            "PYTEST_WORKER": "1",
            "AUTH_TOKEN_STDIN": "1",
            'PTMPROC_VERBOSE': '1' if user_output.verbose else '0',
            'PTMPROC_NODE_MGR_PORT': str(Node.Manager.PORT),
        })
        stdout = sys.stdout
        stderr = sys.stderr
        executable = executable or sys.executable
        proc = subprocess.Popen(
            [executable, '-m', __name__, uri] + sys.argv[1:],
            env=env,
            stdout=stdout, stderr=stderr,
            stdin=subprocess.PIPE)
        proc.stdin.write(binascii.b2a_hex(get_auth_key()) + b'\n')
        proc.stdin.close()
        return proc

    def pytest_internalerror(self, __excrepr):
        self._put(ResultException(SystemError("Internal pytest error")))

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_logstart(self, nodeid, location):
        self._put(ReportStarted(nodeid, location))

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_logfinish(self, nodeid, location):
        self._put(ReportFinished(nodeid, location))

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
    global_mgr_uri = os.environ.get('PTMPROC_GM_URI')
    if global_mgr_uri:
        host, port_text = global_mgr_uri.split(':', maxsplit=1)
        # set host, port for later usage (in global fixtures, etc)
        Global.Manager.singleton(address=(host, int(port_text)), as_client=True)
    mgr = OrchestrationManager.create(uri=uri, as_client=True)
    # noinspection PyUnresolvedReferences
    worker_index = mgr.register_worker((get_ip_addr(), os.getpid()))
    # noinspection PyUnresolvedReferences
    test_q = mgr.get_test_queue().raw()
    # noinspection PyUnresolvedReferences
    result_q = mgr.get_results_queue().raw()
    assert test_q is not None
    assert result_q is not None
    worker = WorkerSession(index=worker_index,
                           is_remote='--as-worker' in sys.argv,
                           test_q=test_q,
                           result_q=result_q,
                           )
    WorkerSession.set_singleton(worker)
    assert WorkerSession.singleton() is not None
    status = None
    # noinspection PyBroadException
    try:
        status = pytest.main(sys.argv[2:])
        if isinstance(status, pytest.ExitCode):
            status = status.value
    except Exception as e:
        errored = True
        msg = f"Worker {get_ip_addr()}-{os.getpid()} died with exception {e}\n {traceback.format_exc()}"
        os.write(sys.stderr.fileno(), msg.encode('utf-8'))
        result_q.put(ClientDied(os.getpid(), get_ip_addr(), errored=errored, message=msg))
    finally:
        mgr.completed(get_ip_addr(), os.getpid())
        with suppress(Exception):
            Node.Manager.shutdown()
    return status


if __name__ == "__main__":
    sys.exit(main())
