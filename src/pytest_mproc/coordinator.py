"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""
import time

from multiprocessing import Semaphore, JoinableQueue, Queue, Process
from multiprocessing.managers import MakeProxyType, SyncManager

from pytest_mproc import find_free_port, AUTHKEY
from pytest_mproc.data import TestExecutionConstraint, TestBatch
from pytest_mproc.fixtures import Node
from pytest_mproc.main import Orchestrator
from pytest_mproc.utils import BasicReporter
from pytest_mproc.worker import WorkerSession


# Create proxies for Queue types to be accessible from remote clients
# NOTE: multiprocessing module has a quirk/bug where passin proxies to another separate client (e.g., on
# another machine) causes the proxy to be rebuilt with a random authkey on the other side.  Unless
# we override the constructor to force an authkey value, we will hit AuthenticationError's

CoordinatorProxyBase = MakeProxyType("CoordinatorProxy", exposed=["start", "put_fixture", "join", "kill", "is_local"])


class CoordinatorProxy(CoordinatorProxyBase):

    def __init__(self, token, serializer, manager=None,
                 authkey=None, exposed=None, incref=True, manager_owned=False):
        super().__init__(token, serializer, manager, AUTHKEY, exposed, incref, manager_owned)


class CoordinatorFactory:

    def __init__(self, num_processes: int, host: str, port: int, max_simultaneous_connections: int,
                 as_remote_client: bool):
        """
        :param num_processes: number of parallel executions to be conducted
        :param host: host name of main node that holds the test queue, result queue, etc
        :param port: port of main node to connect to
        :param max_simultaneous_connections: maximum allowed connections at one time to main node;  throttled to
            prevent overloading *multiprocsesing* module and deadlock
        """
        self._host = host
        self._port = port
        self._num_processes = num_processes
        self._max_simultaneous_connections = max_simultaneous_connections
        self._is_local = not as_remote_client

    def launch(self) -> "Coordinator":
        if not self._is_local:
            self._sm = SyncManager(authkey=AUTHKEY)
            self._sm.start()
            coordinator = self._sm.CoordinatorProxy(self._num_processes, self._host, self._port,
                                                    self._max_simultaneous_connections, self._is_local)
        else:
            coordinator = Coordinator(self._num_processes, self._host, self._port,
                                      self._max_simultaneous_connections, self._is_local)
        client = Orchestrator.Manager(addr=(self._host, self._port))
        client.register_client(coordinator, self._num_processes)
        return coordinator


class Coordinator:
    """
    Context manager for kicking of worker Processes to conduct test execution via pytest hooks
    """
    _node_port = find_free_port()
    _node_manager_client = None

    def __init__(self, num_processes: int, host: str, port: int, max_simultaneous_connections: int, is_local: bool):
        """
        :param num_processes: number of parallel executions to be conducted
        """
        self._is_local = is_local
        self._host = host
        self._port = port
        self._num_processes = num_processes
        self._max_simultaneous_connections = max_simultaneous_connections
        self._count = 0
        self._session_start_time = time.time()
        self._reporter = BasicReporter()
        self._worker_procs = []

    def is_local(self):
        return self._is_local

    def put_fixture(self, name, val) -> None:
        cls = self.__class__
        if cls._node_manager_client is None:
            cls._node_manager_client = Node.Manager(as_main=False, port=cls._node_port)
        cls._node_manager_client.put_fixture(name, val)

    def start(self, test_q: JoinableQueue, result_q: Queue) -> None:
        """
        Start all worker processes

        :return: this object
        """
        local_test_q = JoinableQueue()

        self._node_manager = Node.Manager(as_main=True, port=self.__class__._node_port)
        start_sem = Semaphore(self._max_simultaneous_connections)
        # This will be used to throttle the number of connections made when makeing distributed call to get
        # node-level and global-level fixtures;  otherwise multiproceissing can hang on these calls if
        # overwhelmed
        fixture_sem = Semaphore(self._max_simultaneous_connections)
        for index in range(self._num_processes):
            proc = WorkerSession.start(index, self._host, self._port, start_sem, fixture_sem,
                                       local_test_q, result_q, self._node_port,)
            self._worker_procs.append(proc)
            start_sem.release()
        self._test_q_process = Process(target=self._process_test_q, args=(test_q, local_test_q))
        self._test_q_process.start()

    def _process_test_q(self, source: JoinableQueue, local_test_q: JoinableQueue):
        count = 0
        while count < self._num_processes:
            test_batch = source.get()
            source.task_done()
            if test_batch is not None \
                    and len(test_batch.test_ids) >1 \
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
            proc.join()

    def kill(self):
        for proc in self._worker_procs:
            proc.terminate()
        self._worker_procs = []


SyncManager.register("CoordinatorProxy", Coordinator, CoordinatorProxy)
