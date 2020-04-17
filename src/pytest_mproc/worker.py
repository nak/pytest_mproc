import os
import socket
import sys
import time
from datetime import datetime

import pytest
from _pytest.config import _prepareconfig, ConftestImportFailure
import pytest_cov.embed

if sys.version_info[0] < 3:
    # noinspection PyUnresolvedReferences
    from Queue import Empty
else:
    # noinspection PyUnresolvedReferences
    from queue import Empty

try:
    my_cov = None  # overriden later, after fork
    try:
        original_cleanup = pytest_cov.embed.cleanup
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


class WorkerSession:
    """
    Handles reporting of test status and the like
    """

    def __init__(self, index, result_q, test_generator):
        self._result_q = result_q
        self._index = index
        self._test_generator = test_generator

        self._name = "worker-%d" % (index + 1)
        self._reports = []
        self._count = 0
        self._session_start_time = time.time()
        self._buffered_results = []
        self._buffer_size = 10
        self._timestamp = datetime.utcnow()

    def _put(self, kind, data):
        """
        Append test result data to queue, flushing buffered results to queue at watermark level for efficiency

        :param kind: kind of data
        :param data: test result data to publih to queue
        """
        self._buffered_results.append((kind, data))
        if len(self._buffered_results) >= self._buffer_size or (datetime.utcnow() - self._timestamp).total_seconds() > 1:
            self._flush()

    def _flush(self):
        """
        fluh buffered results out to the queue.
        """
        if self._buffered_results:
            self._result_q.put(self._buffered_results)
            self._buffered_results = []
            self._timestamp = datetime.utcnow()

    def test_loop(self, session):
        """
        This is where the action takes place.  We override the usual implementation since
        that doesn't support a dynamic generation of tests (whose source is the test Queue
        that it draws from to pick out the next test

        :param session:  Where the tests generator is kept
        """
        if pycov_present:
            global my_cov
            try:
                my_cov = pytest_cov.embed.active_cov
            except:
                my_cov = None
        if session.testsfailed and not session.config.option.continue_on_collection_errors:
            raise session.Interrupted("%d errors during collection" % session.testsfailed)

        if session.config.option.collectonly:
            return  # should never really get here, but for consistency

        for preitem in session.items:
            # test item comes through as a unique string nodeid of the test
            # We use the pytest-collected mapping we squirrelled away to look up the
            # actual _pytest.pythone.Function test callable
            for item in preitem if isinstance(preitem, list) else [preitem]:
                item = session._named_items[item]
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

    @pytest.mark.try_first
    def pytest_runtest_logreport(self, report):
        """
        report only status of calls (not setup or teardown), unless there was an error in those
        stash report for later end-game parsing of failures

        :param report: report to draw info from
        """
        self._put('test_status', report)

    def pytest_sessionfinish(self, exitstatus):
        """
        output failure information and final exit status back to coordinator

        :param exitstatus: exit status of the test suite run
        """
        self._put('exit', (self._index, self._count, exitstatus, time.time() - self._session_start_time))
        self._flush()


def main(index, test_q, result_q, num_processes):
    """
    main worker function, launched as a multiprocessing Process

    :param index: index assigned to the worker Process
    :param test_q: to draw test names for execution
    :param result_q: to post status and results
    """
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
        worker = WorkerSession(index, result_q, generator())
        config.pluginmanager.register(worker, "mproc_worker")
        config.option.mproc_worker = worker
        # setup slave info;  this is important to have something defined for these fields
        # so that pycov can pickup on the distributed nature of the test run,
        # and that this a worker(slave) process
        workerinput = {'slaveid': "worker-%d" % index,
                       'workerid': "worker-%d" % index,
                       "workercount": num_processes,
                       "slavecount": num_processes,
                       'cov_master_host': socket.gethostname(),
                       'cov_slave_output': os.path.join(os.getcwd(), "worker-%d" % index),
                       'cov_master_topdir': os.getcwd()
                       }
        config.slaveinput = workerinput
        config.slaveoutput = workerinput
        try:
            # and away we go....
            config.hook.pytest_cmdline_main(config=config)
        finally:
            config._ensure_unconfigure()
    except Exception as e:
        result_q.put(('exception', e))
        result_q.put(('exit', (index, -1, -1)))

