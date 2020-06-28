import os
import socket
import sys
import time
from multiprocessing import Semaphore, JoinableQueue, Queue, Process
from typing import Optional, Any, Dict

import pytest
from _pytest.config import _prepareconfig
import pytest_cov.embed
import resource

from pytest_mproc import resource_utilization
from pytest_mproc.fixtures import Node
from pytest_mproc.utils import BasicReporter

if sys.version_info[0] < 3:
    # noinspection PyUnresolvedReferences
    from Queue import Empty
else:
    # noinspection PyUnresolvedReferences
    from queue import Empty

try:
    my_cov = None  # overriden later, after fork
    try:
        original_cleanup = pytest_cov.embed.cleanupgit
    except:
        original_cleanup = None

    def _cleanup(cov=None):
        # pytest_cov will do special things when tests use multiprocessing, however,
        # here we don't want that processing to take place.  (It will otherwise
        # spit out many lines of messaging all of which has no bearing on success of code coverage
        # collecion)
        if cov != my_cov and original_cleanup:
            original_cleanup(cov)


    pytest_cov.embed.cleanup = _cleanup
    pycov_present = True

except Exception:
    pycov_present = False


"""maximum time between reporting status back to coordinator"""
MAX_REPORTING_INTERVAL = 1.0  # seconds


class WorkerSession:
    """
    Handles reporting of test status and the like
    """

    def __init__(self, index, host: str, port: int, test_q: JoinableQueue, result_q: Queue):
        self._is_remote = host != "127.0.0.1"
        self._host = host
        self._port = port
        self._index = index
        self._name = "worker-%d" % (index + 1)
        self._count = 0
        self._session_start_time = time.time()
        self._buffered_results = []
        self._buffer_size = 20
        self._timestamp = time.time()
        self._last_execution_time = time.time()
        self._resource_utilization = -1, -1, -1, -1
        self._reporter = BasicReporter()
        self._global_fixtures: Dict[str, Any] = {}
        self._node_fixtures: Dict[str, Any] = {}
        self._test_q = test_q
        self._result_q = result_q
        self._node_fixture_manager = None

    def _put(self, kind, data, timeout=None):
        """
        Append test result data to queue, flushing buffered results to queue at watermark level for efficiency

        :param kind: kind of data
        :param data: test result data to publih to queue
        """
        if self._is_remote and kind == 'test_status':
            os.write(sys.stderr.fileno(), b'.')
        self._buffered_results.append((kind, data))
        if len(self._buffered_results) >= self._buffer_size or (time.time() - self._timestamp) > MAX_REPORTING_INTERVAL:
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
        that it draws from to pick out the next test

        :param session:  Where the tests generator is kept
        """
        start_time = time.time()
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        try:
            if pycov_present:
                global my_cov
                try:
                    my_cov = pytest_cov.embed.active_cov
                except:
                    my_cov = None

            if session.testsfailed and not session.config.option.continue_on_collection_errors:
                raise session.Failed("%d errors during collection" % session.testsfailed)

            if session.config.option.collectonly:
                return  # should never really get here, but for consistency
            for items in session.items_generator:
                # test item comes through as a unique string nodeid of the test
                # We use the pytest-collected mapping we squirrelled away to look up the
                # actual _pytest.python.Function test callable
                # NOTE: session.items is an iterator, as wet upon construction of WorkerSession
                for item_name in items:
                    item = session._named_items[item_name]
                    item.config.hook.pytest_runtest_protocol(item=item, nextitem=None)
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
            self._flush()
        finally:
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

        def generator(test_q: JoinableQueue):
            test = test_q.get()
            while test:
                test_q.task_done()
                yield test
                test = test_q.get()
            test_q.task_done()

        session.items_generator = generator(self._test_q)
        session._named_items = {item.nodeid: item for item in session.items}
        return session.items

    @classmethod
    def start(cls, index: int, host: str, port: int, start_sem: Semaphore, fixture_sem: Semaphore,
              test_q: JoinableQueue, result_q: Queue, node_port: int) -> Process:
        """

        :param index: index of worker being created
        :param host: host name of main multiprocessing manager
        :param port: port of main mp manqager
        :param start_sem: gating semaphore used to control # of simultaneous connections (prevents deadlock)
        :param fixture_sem: gating semaphore when querying for test fixtures
        :param test_q: Queue to pull from to get next test to run
        :param result_q: Queue to place results
        :param node_port: port node of localhost manager for node-level fixtures, etc
        :return: Process created for new worker
        """
        proc = Process(target=cls.run, args=(index, host, port, start_sem, fixture_sem, test_q, result_q, node_port, ))
        proc.start()
        return proc

    @staticmethod
    def run(index: int, host: str, port: int, start_sem: Semaphore, fixture_sem: Semaphore,
            test_q: JoinableQueue, result_q: Queue, node_port: int) -> None:
        start_sem.acquire()

        worker = WorkerSession(index, host, port, test_q, result_q)
        worker._node_fixture_manager = Node.Manager(as_main=False, port=node_port, name=f"Worker-{index}")
        worker._fixture_sem = fixture_sem
        args = sys.argv[1:]
        config = _prepareconfig(args, plugins=[])
        # unregister terminal (don't want to output to stdout from worker)
        # as well as xdist (don't want to invoke any plugin hooks from another distribute testing plugin if present)
        config.pluginmanager.unregister(name="terminal")
        # register our listener, and configure to let pycov-test knoew that we are a slave (aka worker) thread
        config.pluginmanager.register(worker, "mproc_worker")
        config.option.mproc_worker = worker
        from pytest_mproc.main import Orchestrator
        worker._client = Orchestrator.Manager(addr=(worker._host, worker._port))
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
        self._put('error_message', excrepr)

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_logreport(self, report):
        """
        report only status of calls (not setup or teardown), unless there was an error in those
        stash report for later end-game parsing of failures

        :param report: report to draw info from
        """
        self._put('test_status', report)
        return True

    @pytest.hookimpl(tryfirst=True)
    def pytest_sessionfinish(self, exitstatus):
        """
        output failure information and final exit status back to coordinator

        :param exitstatus: exit status of the test suite run
        """
        # timeouts on queues sometimes can still have process hang if there is a bottleneck, so use alarm
        self._put('exit', (self._index, self._count, exitstatus, self._last_execution_time - self._session_start_time,
                           self._resource_utilization))
        self._flush()
        return True

    @pytest.hookimpl(tryfirst=True)
    def pytest_fixture_setup(self, fixturedef, request):
        my_cache_key = fixturedef.cache_key(request)
        if not getattr(request.config.option, "mproc_numcores") or fixturedef.scope not in ['node', 'global']:
            return
        if fixturedef.scope == 'node':
            if fixturedef.argname not in self._node_fixtures:
                self._fixture_sem.acquire()
                self._node_fixtures[fixturedef.argname] = self._node_fixture_manager.get_fixture(fixturedef.argname)
                self._fixture_sem.release()
            fixturedef.cached_result = (self._node_fixtures.get(fixturedef.argname),
                                        my_cache_key,
                                        None)
            return self._node_fixtures.get(fixturedef.argname)
        if fixturedef.scope == 'global':
            if fixturedef.argname not in self._global_fixtures:
                self._fixture_sem.acquire()
                self._global_fixtures[fixturedef.argname] = self._client.get_fixture(fixturedef.argname)
                self._fixture_sem.release()
            fixturedef.cached_result = (self._global_fixtures.get(fixturedef.argname),
                                        my_cache_key,
                                        None)
            return self._global_fixtures.get(fixturedef.argname)
