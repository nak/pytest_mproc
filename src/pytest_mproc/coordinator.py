"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""
import os
import sys
import time
from contextlib import suppress

from multiprocessing import current_process
from multiprocessing.managers import SyncManager
from subprocess import TimeoutExpired
from typing import Optional
from pytest_mproc import find_free_port
from pytest_mproc.main import Orchestrator
from pytest_mproc.utils import BasicReporter


def get_ip_addr():
    import socket
    hostname = socket.gethostname()
    # noinspection PyBroadException
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

    def launch(self, host: str, port: int) -> "Coordinator":
        if not self._is_local:
            # noinspection PyUnresolvedReferences
            coordinator = self.sm.Coordinator(self._num_processes,
                                              self._max_simultaneous_connections,
                                              self._is_local)
        else:
            coordinator = Coordinator(self._num_processes,
                                      self._max_simultaneous_connections,
                                      self._is_local)
        # noinspection PyUnresolvedReferences
        self._mgr.register_client(coordinator)
        executable = os.environ.get('PTMPROC_EXECUTABLE', sys.executable)
        coordinator.start(host, port, executable)
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

    def start(self, host: str, port: int, executable: str) -> None:
        """
        Start all worker processes

        :return: this object
        """
        from pytest_mproc.worker import WorkerSession
        for index in range(self._num_processes):
            proc = WorkerSession.start(host, port, executable)
            self._worker_procs.append(proc)
        return

    def join(self, timeout: Optional[float] = None):
        for proc in self._worker_procs:
            try:
                proc.wait(timeout)
            except TimeoutExpired:
                with suppress(OSError):
                    proc.kill()

    def kill(self):
        if CoordinatorFactory.sm:
            CoordinatorFactory.sm.shutdown()
        for proc in self._worker_procs:
            proc.kill()
        self._worker_procs = []


# register the proxy class as the Coordinator class for SyncManager
SyncManager.register("Coordinator", Coordinator)
