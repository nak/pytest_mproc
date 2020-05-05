import os
import socket
import sys
import time

import pytest
from _pytest.config import _prepareconfig, ConftestImportFailure
import pytest_cov.embed
import resource

from pytest_mproc import resource_utilization
from pytest_mproc.config import MPManagerConfig, RoleEnum
from pytest_mproc.fixtures import Global

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

    def __init__(self, index, result_q, test_generator, is_remote: bool):
        self._is_remote = is_remote
        self._result_q = result_q
        self._index = index
        self._test_generator = test_generator

        self._name = "worker-%d" % (index + 1)
        self._reports = []
        self._count = 0
        self._session_start_time = time.time()
        self._buffered_results = []
        self._buffer_size = 20
        self._timestamp = time.time()
        self._last_execution_time = time.time()
        self._resource_utilization = -1, -1, -1, -1

    def _put(self, kind, data):
        """
        Append test result data to queue, flushing buffered results to queue at watermark level for efficiency

        :param kind: kind of data
        :param data: test result data to publih to queue
        """
        if self._is_remote and kind == 'test_status':
            os.write(sys.stderr.fileno(), b'.')
        self._buffered_results.append((kind, data))
        if len(self._buffered_results) >= self._buffer_size or (time.time() - self._timestamp) > MAX_REPORTING_INTERVAL:
            self._flush()

    def _flush(self):
        """
        fluh buffered results out to the queue.
        """
        if self._buffered_results:
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
            for items in session.items:
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
        session._named_items = {item.nodeid: item for item in session.items}
        session.items = self._test_generator
        return session.items

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
        self._put('exit', (self._index, self._count, exitstatus, self._last_execution_time - self._session_start_time,
                           self._resource_utilization))
        self._flush()
        return True


def main(index, mpconfig: MPManagerConfig, is_remote: bool):
    """
    main worker function, launched as a multiprocessing Process

    :param index: index assigned to the worker Process
    """
    client = Global.Manager(mpconfig, force_as_client=True)
    test_q = client.get_test_queue()
    result_q = client.get_results_queue()
    args = sys.argv[1:]
    try:
        def generator():
            try:
                test = test_q.get()
                while test:
                    yield test
                    test = test_q.get()
            except Empty:
                raise StopIteration

        try:
            # use pytest's usual method to prepare configuration
            # we have to re-generate the config in this worker Process, as we cannot pickle the more complex
            # _pytest.python.Function objects across the Queue interface.  This is a slight bit
            # of awkwardness and some overhead per process, but this overhead pales in significance
            # to xdist's use of rsync
            config = _prepareconfig(args, plugins=[])
        except ConftestImportFailure as e:
            result_q.put(('exception', e))
        # unregister terminal (don't want to output to stdout from worker)
        # as well as xdist (don't want to invoke any plugin hooks from another distribute testing plugin if present)
        config.pluginmanager.unregister(name="terminal")
        # register our listener, and configure to let pycov-test knoew that we are a slave (aka worker) thread
        worker = WorkerSession(index, result_q, generator(), is_remote)
        config.pluginmanager.register(worker, "mproc_worker")
        config.option.mproc_worker = worker
        # setup slave info;  this is important to have something defined for these fields
        # so that pycov can pickup on the distributed nature of the test run,
        # and that this a worker(slave) process
        workerinput = {'slaveid': "worker-%d" % index,
                       'workerid': "worker-%d" % index,
                       "workercount": mpconfig.num_processes,
                       "slavecount": mpconfig.num_processes,
                       'cov_master_host': socket.gethostname(),
                       'cov_slave_output': os.path.join(os.getcwd(), "worker-%d" % index),
                       'cov_master_topdir': os.getcwd()
                       }
        config.slaveinput = workerinput
        config.slaveoutput = workerinput
        config.option.mpconfig = mpconfig
        assert(mpconfig.role == RoleEnum.WORKER)
        try:
            # and away we go....
            config.hook.pytest_cmdline_main(config=config)
        finally:
            config._ensure_unconfigure()
    except Exception as e:
        result_q.put(('exception', e))
        result_q.put(('exit', (index, -1, -1, (-1. -1, -1, -1))))
