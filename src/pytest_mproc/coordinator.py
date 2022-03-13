"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""
import os
import time

from multiprocessing import Semaphore, JoinableQueue, Queue, Process
from multiprocessing import current_process
from multiprocessing.managers import SyncManager
from typing import Optional

import pytest

from pytest_mproc import find_free_port
from pytest_mproc.data import TestExecutionConstraint, TestBatch
from pytest_mproc.fixtures import Node
from pytest_mproc.main import Orchestrator
from pytest_mproc.utils import BasicReporter
from pytest_mproc.worker import WorkerSession


def get_ip_addr():
    import socket
    hostname = socket.gethostname()
    try:
        return socket.gethostbyname(hostname)
    except Exception:
        return None


class CoordinatorFactory:

    sm: Optional[SyncManager] = None

    def __init__(self, num_processes: int, mgr: Orchestrator.Manager,
                 max_simultaneous_connections: int,
                 as_remote_client: bool):
        """
        :param num_processes: number of parallel executions to be conducted
        :param mgr: global manager of queues, etc.
        :param max_simultaneous_connections: maximum allowed connections at one time to main node;  throttled to
            prevent overloading *multiprocsesing* module and deadlock
        :param as_remote_client: whether running as remote client (or on local host if False)
        """
        self._num_processes = num_processes
        self._max_simultaneous_connections = max_simultaneous_connections
        self._is_local = not as_remote_client
        if not self._is_local:
            self.sm = SyncManager(authkey=current_process().authkey, address=(get_ip_addr(), find_free_port()))
            self.sm.start()
        self._mgr = mgr

    def launch(self) -> "Coordinator":
        if not self._is_local:
            coordinator = self.sm.Coordinator(self._num_processes,
                                              self._max_simultaneous_connections,
                                              self._is_local)
        else:
            coordinator = Coordinator(self._num_processes,
                                      self._max_simultaneous_connections,
                                      self._is_local)
        print(f">>>>>>>>>>>>>>>>>> MGR IS {self._mgr}")
        self._mgr.register_client(coordinator, self._num_processes)
        return coordinator


class Coordinator:
    """
    Context manager for kicking off worker Processes to conduct test execution via pytest hooks
    """

    def __init__(self, num_processes: int, max_simultaneous_connections: int, is_local: bool):
        """
        :param num_processes: number of parallel executions to be conducted
        """
        self._is_local = is_local
        self._num_processes = num_processes
        self._max_simultaneous_connections = max_simultaneous_connections
        self._count = 0
        self._session_start_time = time.time()
        self._reporter = BasicReporter()
        self._worker_procs = []

    def is_local(self):
        return self._is_local

    def put_fixture(self, name, val) -> None:
        Node.Manager.singleton().put_fixture(name, val)

    def get_fixture(self, name, val) -> None:
        Node.Manager.singleton().get_fixture(name, val)

    def start(self, host: str, port: int) -> None:  #test_q: JoinableQueue, result_q: Queue) -> None:
        """
        Start all worker processes

        :return: this object
        """
        for index in range(self._num_processes):
            proc = WorkerSession.start(index, host, port)
            self._worker_procs.append(proc)
        return

    def _process_test_q(self, source: JoinableQueue, local_test_q: JoinableQueue):
        count = 0
        while count < self._num_processes:
            test_batch = source.get()
            source.task_done()
            if test_batch is not None \
                    and len(test_batch.test_ids) > 1 \
                    and test_batch.restriction == TestExecutionConstraint.SINGLE_NODE:
                for test_id in test_batch.test_ids:
                    local_test_q.put(TestBatch([test_id]))
                    local_test_q.join()
            else:
                local_test_q.put(test_batch)
                local_test_q.join()
            if test_batch is None:
                count += 1

        local_test_q.close()

    def join(self):
        for proc in self._worker_procs:
            proc.wait()

    def kill(self):
        if CoordinatorFactory.sm:
            CoordinatorFactory.sm.shutdown()
        for proc in self._worker_procs:
            proc.kill()
        self._worker_procs = []

    @pytest.hookimpl(tryfirst=True)
    def pytest_fixture_setup(self, fixturedef, request):
        self._node_fixtures = {}
        self._global_fixtures = {}
        my_cache_key = fixturedef.cache_key(request)
        if request.config.option.ptmproc_config.num_cores is None or fixturedef.scope not in ['node', 'global']:
            return
        if fixturedef.scope == 'node':
            if fixturedef.argname not in self._node_fixtures:
                self._fixture_sem.acquire()
                self._node_fixtures[fixturedef.argname] = Node.Manager.singleton().get_fixture(fixturedef.argname)
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



# register the proxy class as the Coordinator class for SyncManager
SyncManager.register("Coordinator", Coordinator)
