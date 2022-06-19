"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""
import os
import subprocess
import sys
import time
from contextlib import suppress

from multiprocessing.managers import SyncManager
from subprocess import TimeoutExpired
from typing import Optional, List

from pytest_mproc.utils import BasicReporter
from pytest_mproc.worker import WorkerSession


class Coordinator:
    """
    Context manager for kicking off worker Processes to conduct test execution via pytest hooks
    Coordinators are scoped to a node and only handle a collection of workers on that node
    """

    _singleton: Optional["Coordinator"] = None

    def __init__(self):
        """
        """
        assert Coordinator._singleton is None, "Attempt to instantiate Coordinator more than once on same machine"
        self._count = 0
        self._session_start_time = time.time()
        self._reporter = BasicReporter()
        self._worker_procs: List[subprocess.Popen] = []

    @classmethod
    def singleton(cls) -> "Coordinator":
        """
        :return: sole (possibly created) singleton instance of this class
        """
        if cls._singleton is None:
            cls._singleton = Coordinator()
        return cls._singleton

    def start_workers(self, uri: str, num_processes: int) -> None:
        """
        State a worker

        :param uri: uri of server that manages the test and result queues
        :param num_processes: number of worker processes to launch
        """
        executable = os.environ.get('PTMPROC_EXECUTABLE', sys.executable)
        for index in range(num_processes):
            proc = WorkerSession.start(uri, executable)
            self._worker_procs.append(proc)
        time.sleep(1)
        for index, proc in enumerate(self._worker_procs):
            if proc.returncode is not None:
                raise SystemError(f"Worker-{index} failed to start")

    def shutdown(self, timeout: Optional[float] = None) -> None:
        """
        shutdown servers and clean up
        :param timeout: timeout if taking too long
        :raises: TimeoutError if taking too long
        """
        for proc in self._worker_procs:
            try:
                proc.wait(timeout=timeout)
            except TimeoutExpired:
                with suppress(OSError):
                    proc.kill()
        self._worker_procs = []

    def kill(self) -> None:
        """
        Abruptly kill all worker processes
        """
        for proc in self._worker_procs:
            proc.kill()
        self._worker_procs = []


# register the proxy class as the Coordinator class for SyncManager
SyncManager.register("Coordinator", Coordinator, exposed=["start", "shutdown", "kill"])
