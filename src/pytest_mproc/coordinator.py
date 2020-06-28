"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""
import time

from multiprocessing import Semaphore, JoinableQueue, Queue
from typing import Dict, Any

from pytest_mproc import find_free_port
from pytest_mproc.fixtures import Node
from pytest_mproc.main import Orchestrator
from pytest_mproc.utils import BasicReporter
from pytest_mproc.worker import WorkerSession


class Coordinator:
    """
    Context manager for kicking of worker Processes to conduct test execution via pytest hooks
    """

    def __init__(self, num_processes: int, host: str, port: int, max_simultaneous_connections):
        """
        :param num_processes: number of parallel executions to be conducted
        """
        self._host = host
        self._port = port
        self._num_processes = num_processes
        self._max_simultaneous_connections = max_simultaneous_connections
        # test_q is for sending test nodeid's to worked
        # result_q is for receiving results as messages, exceptions, test status or any exceptions thrown
        self._count = 0
        self._session_start_time = time.time()
        self._reporter = BasicReporter()
        self._worker_procs = []
        self._node_port = find_free_port()
        assert self._node_port != port
        node_manager = Node.Manager(as_main=True, port=self._node_port)
        client = Orchestrator.Manager(addr=(host, port))
        client.register_client(self, self._num_processes)
        self._node_manager = node_manager

    def put_fixture(self, name, val) -> None:
        self._node_manager.put_fixture(name, val)

    def fixtures(self) -> Dict[str, Any]:
        return self._node_manager._fixtures

    def start(self, test_q: JoinableQueue, result_q: Queue) -> None:
        """
        Start all worker processes

        :return: this object
        """
        start_sem = Semaphore(self._max_simultaneous_connections)
        # This will be used to throttle the number of connections made when makeing distributed call to get
        # node-level and global-level fixtures;  otherwise multiproceissing can hang on these calls if
        # overwhelmed
        fixture_sem = Semaphore(self._max_simultaneous_connections)
        for index in range(self._num_processes):
            proc = WorkerSession.start(index, self._host, self._port, start_sem, fixture_sem,
                                       test_q, result_q, self._node_port,)
            self._worker_procs.append(proc)
            start_sem.release()

    def join(self):
        for proc in self._worker_procs:
            proc.join()

    def kill(self):
        for proc in self._worker_procs:
            proc.terminate()
        self._worker_procs = []



