"""
This package contains the code to handle the RPC calls to a single manager that holds the elements
important to distributed processing:  a single test and result queue, semaphores for control, etc.
"""
import multiprocessing
import os
import signal
from contextlib import suppress
from enum import Enum
from multiprocessing import Semaphore
from multiprocessing import JoinableQueue
from multiprocessing.managers import RemoteError, BaseManager
from typing import Optional, Dict, Tuple

from pytest_mproc import get_auth_key, Settings, AsyncMPQueue
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.remote.ssh import SSHClient
from pytest_mproc.user_output import always_print


class Protocol(Enum):
    """
    Supported protocols for the manager
    """
    LOCAL = 'local'
    # local host
    REMOTE = 'remote'
    # explicit host ip address


# rough estimate of number of results published for each test run by a worker
# (e.g., test-report-started, test-status, test-report, test-report-finished messages)
_RESPONSES_TO_TEST_RATIO = 10


class OrchestrationMPManager(BaseManager):
    """
    This base class provides the underlying multiprocessing server interface to connect with or start the server,
    and the API for interacting with the server that is registered as the Orchestration API
    """

    class Value:
        def __init__(self, val):
            self._val = val

        def value(self):
            return self._val

    def __init__(self, host: str, port: int):
        super().__init__((host, port), authkey=get_auth_key())
        OrchestrationMPManager.register("Queue", multiprocessing.Queue, exposed=["get", "put"])
        self._port = port
        self._host = host
        # only for SSH protocol:
        self._workers: Dict[str, Tuple[str, int]] = {}
        self._is_server = False
        self._mproc_pid: Optional[int] = None
        self._remote_pid: Optional[int] = None
        self._finalize_sem = multiprocessing.Semaphore(0)
        self._test_q = JoinableQueue(50)
        self._results_q = JoinableQueue(50 * _RESPONSES_TO_TEST_RATIO)
        self._coordinator_q: Dict[str, JoinableQueue] = {}
        self._finalize_sem = Semaphore(0)
        self._all_tests_sent = False

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def delegated(self):
        return self._port == -1

    def do_connect(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        """
        Connect as a client to an existing server.

        :param host: if host assignment deferred, use this it now, otherwise ignored
        :param port: if port assignment deferred, use this to set it now, otherwise ignored
        """
        if self._host is not None and host is not None:
            raise ValueError("Cannot specify host on connect when one already assigned on construction")
        elif self._host is None and host is None:
            raise ValueError(f"No host set")
        elif self._host is None:
            self._host = host
            self._port = port
        OrchestrationMPManager.register("JoinableQueue")
        OrchestrationMPManager.register("Semaphore")
        OrchestrationMPManager.register("join")
        OrchestrationMPManager.register("finalize")
        OrchestrationMPManager.register("signal_all_tests_sent")
        OrchestrationMPManager.register("register_worker")
        OrchestrationMPManager.register("get_coordinator_q")
        OrchestrationMPManager.register("count_raw")
        OrchestrationMPManager.register("completed")
        # noinspection SpellCheckingInspection
        OrchestrationMPManager.register("get_test_queue_raw")
        OrchestrationMPManager.register("get_results_queue_raw")
        self.connect()

    def do_start(self):
        """
        Start this instance as the main server
        """
        OrchestrationMPManager.register("join", self._join)
        OrchestrationMPManager.register("finalize", self._finalize)
        OrchestrationMPManager.register("signal_all_tests_sent", self._signal_all_tests_sent)
        OrchestrationMPManager.register("register_worker", self._register_worker)
        OrchestrationMPManager.register("get_coordinator_q", self._get_coordinator_q)
        OrchestrationMPManager.register("count_raw", self._count)
        OrchestrationMPManager.register("completed", self._completed)
        # noinspection SpellCheckingInspection
        OrchestrationMPManager.register("JoinableQueue", JoinableQueue,
                                        exposed=["put", "get", "task_done", "join", "close", "get_nowait"])
        OrchestrationMPManager.register("Semaphore", Semaphore, exposed=["acquire", "release"])
        OrchestrationMPManager.register("get_test_queue_raw", self._get_test_queue)
        OrchestrationMPManager.register("get_results_queue_raw", self._get_results_queue)
        # we only need these as dicts when running a standalone remote server handling multiple
        # pytest sessions/users
        always_print(f"Starting orchestration on {self._host}:{self._port}")
        self.start()
        self._is_server = True

    def _join(self,  timeout: Optional[float] = None) -> None:
        """
        For server, join all existing clients, presumably when expecting client to complete soon
        For client, do nothing

        :param timeout: Optional timeout to wait
        :raises: TimeoutError if timeout is specified and reached before join completes
        """
        for client in self._coordinator_q.values():  # empty if client
            with suppress(ConnectionRefusedError, RemoteError):
                # noinspection PyUnresolvedReferences
                client.put(('shutdown', timeout))

    def shutdown(self) -> None:
        self.join(timeout=5)
        pid_map = {}
        for (host, pid) in self._workers.values():
            pid_map.setdefault(host, []).append(pid)
        for host, pids in pid_map.items():
            ssh_client = SSHClient(host=host, username=Settings.ssh_username, password=Settings.ssh_password)
            with suppress(Exception):
                ssh_client.signal_sync(pids, signal.SIGINT)
        if self._remote_pid is not None:
            with suppress(Exception):
                os.kill(self._remote_pid, signal.SIGINT)
            with suppress(Exception):
                os.kill(self._remote_pid, signal.SIGKILL)
        if self._mproc_pid is not None:
            with suppress(Exception):
                os.kill(self._mproc_pid, signal.SIGINT)
            with suppress(Exception):
                os.kill(self._mproc_pid, signal.SIGKILL)

    #####################
    # internal server API;  these methods are registered as prat of the interface, only
    # without the leading underscore
    #####################

    def _get_results_queue(self) -> JoinableQueue:
        return self._results_q

    def _get_coordinator_q(self, host_: str) -> JoinableQueue:
        """
        :param host_: host to search under
        :return: coordinator associated with host or None
        """
        self._coordinator_q.setdefault(host_, JoinableQueue(10))
        return self._coordinator_q[host_]

    def _register_worker(self, worker: Tuple[str, int]) -> "OrchestrationMPManager.Value":
        """
        Register a worker
        :param worker: worker to register
        :return: unique index of worker (as Value for multiprocessing purposes)
        """
        ip_addr, pid = worker
        worker_key = f"{ip_addr}-{pid}"
        self._workers[worker_key] = worker
        if self._all_tests_sent:
            self._test_q.put(None)   # will signal worker to end, with no tests to run
        return self.Value(len(self._workers))

    def _count(self):
        """
        :return: count of workers currently registered
        """
        return self.Value(len(self._workers))

    def _completed(self, host: str, index: int):
        """
        Signal the worker is completed
        :param host: which host worker is on
        :param index: index of worker
        :return:
        """
        key = f"{host}-{index}"
        if key in self._workers:
            del self._workers[key]

    def _get_test_queue(self):
        return self._test_q

    def _signal_all_tests_sent(self):
        """
        Signal that main node has sent all tests to the queue.  This will in turn signal all workers that
        no more tests remain once the queue is drained
        """
        self._all_tests_sent = True

    def _finalize(self):
        self._finalize_sem.release()


class OrchestrationManager:
    """
    Class to hold the test queue, results queue, and control interface for shutdown, etc.
    """

    def __init__(self, *args, **kwargs):
        self._mp_manager = OrchestrationMPManager(*args, **kwargs)

    @property
    def host(self):
        return self._mp_manager.host

    @property
    def port(self):
        return self._mp_manager.port

    @property
    def delegated(self):
        return self._mp_manager.delegated

    @staticmethod
    def create_server(address: Tuple[str, int]):
        mgr = OrchestrationManager(address[0], address[1])
        mgr.start()
        return mgr

    @staticmethod
    def create_client(address: Tuple[str, int]) -> "OrchestrationManager":
        """
        create an orchestration manager to manage test & results q's as well as clients/workers
        one of three protocols can be used in uri: local for running a server local,
        remote for connecting to a remote server already running, os ssh (in which case host can be
        in form of a destination: user@host).  In the latter it will launch a remote instance
        to the specified host of the server.  Note that running locally means that a port
        must be opened for incoming requests.  Firewalls can prevent this, hence the other
        protocol options

        :param address:  host, port tuple to connect to
        :return: OrchestrationManager instance
        """
        host, port = address
        mgr = OrchestrationManager(host, port)
        mgr.connect()
        return mgr

    def connect(self, host: Optional[str] = None, port: Optional[int] = None):
        """
        Connect to OrchestrationManager
        :param host: if host assignment deferred, which host manager is on, otherwise ignored
        :param port: if port assignment deferred, which port manager is no, otherwise ignored
        """
        self._mp_manager.do_connect(host, port)

    def start(self):
        """
        Start the OrchestrationManager as a server
        """
        self._mp_manager.do_start()

    def get_test_queue(self) -> AsyncMPQueue:
        # noinspection PyUnresolvedReferences
        return AsyncMPQueue(self._mp_manager.get_test_queue_raw())

    def get_results_queue(self) -> AsyncMPQueue:
        # noinspection PyUnresolvedReferences
        return AsyncMPQueue(self._mp_manager.get_results_queue_raw())

    def count(self) -> int:
        """
        :return: number of currently registered workers
        """
        # noinspection PyUnresolvedReferences
        return self._mp_manager.count_raw().value()

    def completed(self, host: str, index: int):
        """
        signal that worker is completed
        :param host: which host worker was on
        :param index: which index was assigned to that worker
        """
        # noinspection PyUnresolvedReferences
        return self._mp_manager.completed(host, index)

    def signal_all_tests_sent(self):
        """
        Signal that all tests have been placed in the queue
        """
        # noinspection PyUnresolvedReferences
        return self._mp_manager.signal_all_tests_sent()

    def get_coordinator(self, host) -> Coordinator.ClientProxy:
        # noinspection PyUnresolvedReferences
        q = self._mp_manager.get_coordinator_q(host)
        return Coordinator.ClientProxy(q, host)

    def get_coordinator_q(self, host) -> JoinableQueue:
        # noinspection PyUnresolvedReferences
        return self._mp_manager.get_coordinator_q(host)

    def register_worker(self, worker: Tuple[str, int]) -> int:
        """
        Register a worker with given tuple of (host, index)

        :param worker: unique host/index combo to identify worker
        """
        # noinspection PyUnresolvedReferences
        return self._mp_manager.register_worker(worker).value()

    def join(self, timeout: Optional[float] = None):
        return self._mp_manager.join(timeout)

    def shutdown(self):
        return self._mp_manager.shutdown()
