import resource
import socket
import sys
import time
from contextlib import suppress
from multiprocessing.managers import MakeProxyType

import _pytest.fixtures
from multiprocessing import JoinableQueue, Process, Queue
from multiprocessing.managers import SyncManager, RemoteError
from typing import List, Any, Optional, Tuple

import pytest

from pytest_mproc import resource_utilization, find_free_port
from pytest_mproc.fixtures import Global, FixtureManager
from pytest_mproc.utils import BasicReporter

__all__ = ["Orchestrator"]


def _localhost():
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        print(">>> Cannot get ip address of host.  Return 127.0.0.1 (localhost)")
        return "127.0.0.1"


# Create proxies for Queue types to be accessible from remote clients
# NOTE: multiprocessing module has a quirk/bug where passin proxies to another separate client (e.g., on
# another machine) causes the proxy to be rebuilt with a random authkey on the other side.  Unless
# we override the constructor to force an authkey value, we will hit AuthenticationError's

JoinableQueueProxyBase = MakeProxyType("JoinableQueueProxy", exposed=["put", "get", "task_done", "join", "close"])
QueueProxyBase = MakeProxyType("QueueProxy", exposed=["put", "get", "task_done", "join", "close"])


class JoinableQueueProxy(JoinableQueueProxyBase):

    def __init__(self, token, serializer, manager=None,
                 authkey=None, exposed=None, incref=True, manager_owned=False):
        super().__init__(token, serializer, manager, b'pass', exposed, incref, manager_owned)


class QueueProxy(QueueProxyBase):

    def __init__(self, token, serializer, manager=None,
                 authkey=None, exposed=None, incref=True, manager_owned=False):
        super().__init__(token, serializer, manager, b'pass', exposed, incref, manager_owned)


class Orchestrator:
    """
    class that acts as Main point of orchestration
    """

    class Manager(FixtureManager):

        class Value:
            def __init__(self, val):
                self._val = val
            def value(self):
                return self._val

        def __init__(self, main: Optional["Orchestrator"] = None, addr: Optional[Tuple[str, int]] = None):
            if not main:
                # client
                Orchestrator.Manager.register("register_client")
                Orchestrator.Manager.register("count")
                Orchestrator.Manager.register("finalize")
                Orchestrator.Manager.register("JoinableQueueProxy")
            else:
                # server:
                self._worker_count = 0
                self._clients = []
                self._orchestrator = main
                self._finalized = False
                Orchestrator.Manager.register("register_client", self._register_client)
                Orchestrator.Manager.register("count", self._count)
                Orchestrator.Manager.register("finalize", self._finalize)
                Orchestrator.Manager.register("JoinableQueueProxy", JoinableQueue, JoinableQueueProxy)
            addr = (main.host, main.port) if main else addr
            super().__init__(addr=addr, as_main=main is not None, passw='pass')

        def _register_client(self, client, count: int) -> Value:
            if self._finalized:
                raise Exception("Client registered after disconnect")
            self._clients.append(client)
            self._worker_count += count
            client.start(self._orchestrator._test_q, self._orchestrator._result_q)

        def _count(self):
            self._finalized = True
            return self.Value(self._worker_count)

        def _finalize(self):
            for client in self._clients:
                client.join()
            self._clients = []

    def __init__(self, host: str = _localhost(), port = find_free_port(), is_serving_remotes: bool=False):
        """
        :param num_processes: number of parallel executions to be conducted
        """
        self._tests = []  # set later
        self._count = 0
        self._rusage = []
        self._session_start_time = time.time()
        if is_serving_remotes:
            SyncManager.register("JoinableQueueProxy", JoinableQueue, JoinableQueueProxy)
            SyncManager.register("QueueProxy", Queue, QueueProxy)
            self._queue_manager = SyncManager(authkey=b'pass')
            self._queue_manager.start()
            self._test_q: JoinableQueue = self._queue_manager.JoinableQueueProxy()
            self._result_q: Queue = self._queue_manager.QueueProxy()
        else:
            self._test_q: JoinableQueue = JoinableQueue()
            self._result_q: Queue = Queue()
        self._reporter = BasicReporter()
        self._exit_q = JoinableQueue()
        self._host = host
        self._port = port
        self._mp_manager = self.Manager(self)
        self._is_serving_remotes = is_serving_remotes

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

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
        for index, (count, duration, ucpu, scpu, mem) in enumerate(self._rusage):
            if count > 0:
                sys.stdout.write(f"Process Worker-{index} executed {count} tests in {duration:.2f} " +\
                                 f"seconds; User CPU: {ucpu:.2f}%, Sys CPU: {scpu:.2f}%, " +\
                                 f"Mem consumed: {unshared_mem/1000.0}M\n")
            else:
                sys.stdout.write(f"Process Worker-{index} executed 0 tests\n")
        sys.stdout.write("\n")
        sys.stdout.write(
            f"Process Coordinator executed in {time_span:.2f} seconds. " +
            f"User CPU: {ucpu:.2f}%, Sys CPU: {scpu:.2f}%, " +
            f"Mem consumed: {unshared_mem/1000.0}M\n"
        )
        length = sum([1 if not isinstance(t, list) else len(t) for t in self._tests])
        if self._count != length:
            self._write_sep('!', "{} tests unaccounted for {} out of {}".format(length - self._count,
                            self._count, length))
        sys.stdout.flush()

    def _process_worker_message(self, hook, typ, data, ):
        """
        Process a message (as a worker) from the coordinating process

        :param typ: the kind of messsage
        :param data: payload for the message to be processed
        """
        try:
            # TODO: possibly have more structured communications, but for now
            # there are limited types of messages, so sendo message, <data> tuple
            # seems to be fine?
            if typ == 'test_status':
                report = data
                if report.when == 'call' or (report.when == 'setup' and not report.passed):
                    self._count += 1
                hook.pytest_runtest_logreport(report=report)
                if report.failed:
                    sys.stdout.write("\n%s FAILED\n" % report.nodeid)
            elif typ == 'exit':
                # process is complete, so close it and set to None
                try:
                    index, worker_count, exitstatus, duration, rusage = data  # which process and count of tests run
                    time_span, ucpu, scpu, unshared_mem = rusage
                    self._rusage.append((worker_count, time_span, ucpu, scpu, unshared_mem))
                    self._exit_q.put(index)
                except:
                    pass
                    # self._reporter.write(f"Unable to process rusage: {data}\n")
            elif typ == 'error_message':
                error_msg_text = data
                sys.stdout.write("{}\n".format(error_msg_text))
            elif typ == 'exception':
                # reraise any exception from workers
                raise Exception("Exception in worker process: %s" % str(data)) from data

        except Exception as e:
            import traceback
            traceback.print_exc()
            sys.stdout.write("INTERNAL_ERROR> %s\n" % str(e))

    def put_fixture(self, name, value):
        self._mp_manager.put_fixture(name, value)

    def fixtures(self):
        return self._mp_manager._fixtures

    def read_results(self, hook):
        try:
            items = self._result_q.get()
            while items is not None:
                if isinstance(items, Exception):
                    raise Exception
                for kind, data in items:
                    self._process_worker_message(hook, kind, data)
                items = self._result_q.get()
        except OSError:
            pass
        finally:
            self._result_q.close()

    def populate_test_queue(self, tests: List[Any], end_sem):
        try:
            count = 0
            for test in tests:
                # Function objects in pytest are not pickle-able, so have to send string nodeid and
                # do lookup on worker side
                item = [t.nodeid for t in test] if isinstance(test, list) else [test.nodeid]
                self._test_q.put(item)
                count += 1
                if count % 20 == 0 or count >= len(tests):
                    self._test_q.join()
            client = self.Manager(addr=(self._host, self._port))
            worker_count = client.count().value()
            for index in range(worker_count):
                self._test_q.put(None)
                self._test_q.join()
            self._test_q.close()
            with suppress(RemoteError):
                client.finalize()
            self._result_q.put(None)
        finally:
            if end_sem:
                end_sem.release()

    def set_items(self, tests):
        """
        :param tests: the items containing the pytest hooks to the tests to be run
        """
        grouped = [t for t in tests if getattr(t._pyfuncitem.obj, "_pytest_group", None)]
        self._tests = [t for t in tests if t not in grouped]
        groups = {}
        for g in grouped:
            name, priority = g._pyfuncitem.obj._pytest_group
            groups.setdefault((name, priority), []).append(g)
        for key in sorted(groups.keys(), key=lambda x: x[1]):
            self._tests.insert(0, groups[key])

    def run_loop(self, session):
        """
        Populate test queue and continue to process messages from worker Processes until they complete

        :param session: Pytest test session, to get session or config information
        """
        start_rusage = resource.getrusage(resource.RUSAGE_SELF)
        start_time = time.time()
        # we are the root node, so populate the tests
        populate_tests_process = Process(target=self.populate_test_queue, args=(self._tests, None))
        populate_tests_process.start()
        self.read_results(session.config.hook)  # only master will read results and post reports through pytest
        populate_tests_process.join(timeout=1)  # should never time out since workers are done
        end_rusage = resource.getrusage(resource.RUSAGE_SELF)
        self._reporter.write("Shutting down..")
        self._mp_manager.shutdown()
        self._reporter.write("Shut down")
        time_span = time.time() - start_time
        time_span, ucpu, scpu, addl_mem_usg = resource_utilization(time_span=time_span,
                                                                   start_rusage=start_rusage,
                                                                   end_rusage=end_rusage)

        sys.stdout.write("\r\n")
        self._output_summary(time_span, ucpu, scpu, addl_mem_usg)

    @pytest.hookimpl(tryfirst=True)
    def pytest_fixture_setup(self, fixturedef, request, _pytset=None):
        result = _pytset.fixtures.pytest_fixture_setup(fixturedef, request)
        if fixturedef.scope == 'global':
            self._mp_manager.put(fixturedef.argname, result)
        return result
