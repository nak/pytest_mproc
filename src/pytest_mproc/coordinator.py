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
from typing import Optional, List, Dict

from pytest_mproc.user_output import always_print
from pytest_mproc.fixtures import Global
from pytest_mproc.worker import WorkerSession
from pytest_mproc.utils import BasicReporter


class Coordinator:
    """
    Context manager for kicking off worker Processes to conduct test execution via pytest hooks
    Coordinators are scoped to a node and only handle a collection of workers on that node
    """

    _singleton: Optional["Coordinator"] = None

    def __init__(self, global_mgr_port: int, orchestration_port: int):
        """
        """
        assert Coordinator._singleton is None, "Attempt to instantiate Coordinator more than once on same machine"
        self._count = 0
        self._global_mgr_port = global_mgr_port
        self._orchestration_port = orchestration_port
        self._session_start_time = time.time()
        self._reporter = BasicReporter()
        self._worker_procs: List[subprocess.Popen] = []

    def start_worker(self, index: int, args: List[str], addl_env: Dict[str, str]) -> int:
        """
        start a single worker based on config
        :param args: additional arguments to pass to worker proces
        :param addl_env: additional environment variables to set for worker process
        :param index: unique index of worker

        :return: pid of created process (must be pickleable, so don't return Popen instance!)
        """
        try:
            executable = os.environ.get('PTMPROC_EXECUTABLE', sys.executable)
            proc = WorkerSession.start(
                orchestration_port=self._orchestration_port,
                global_mgr_port=self._global_mgr_port,
                executable=executable,
                args=args,
                addl_env=addl_env
            )
            self._worker_procs.append(proc)
            if proc.returncode is not None:
                raise SystemError(f"Worker-{index} failed to start")
            return proc.pid
        except Exception as e:
            raise SystemError(f"Failed to launch Worker-{index}: {e}")

    def start_workers(self, num_processes: int,
                      addl_env: Optional[Dict[str, str]] = None,
                      args: Optional[List[str]] = None) -> List[int]:
        """
        State a worker

        :param num_processes: number of worker processes to launch
        :param addl_env: optional additional environ variable for worker processes
        :param args: optional additional args to pass to worker processes
        """
        pids = []
        for index in range(num_processes):
            pids.append(self.start_worker(index, args=args, addl_env=addl_env))
        time.sleep(0.5)
        for index, proc in enumerate(self._worker_procs):
            if proc.returncode is not None:
                always_print(f"Worker-{index} failed to start")
                self._worker_procs.remove(proc)
                pids.remove(pids[index])
        if not self._worker_procs:
            raise SystemError("All workders failed to start")
        return pids

    def wait(self, timeout: Optional[float] = None):
        for proc in self._worker_procs:
            proc.wait(timeout=timeout)

    def shutdown(self, timeout: Optional[float] = None) -> None:
        """
        shutdown servers and clean up
        :param timeout: timeout if taking too long
        :raises: TimeoutError if taking too long
        """
        for proc in self._worker_procs:
            try:
                proc.terminate()
                proc.wait(timeout=timeout)
            except (TimeoutExpired, KeyboardInterrupt):
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


def coordinator_main(global_mgr_port: int, orchestration_port: int, host: str) -> Coordinator:
    """
    entry point for running as main
    """
    from pytest_mproc.orchestration import OrchestrationManager, OrchestrationMPManager
    Global.Manager.singleton(address=('localhost', global_mgr_port), as_client=True)
    mgr = OrchestrationManager.create_client(address=('localhost', orchestration_port))
    OrchestrationMPManager.register("Coordinator", Coordinator, exposed=["start_workers", "start_worker",
                                                                         "kill", "shutdown"])
    coordinator = mgr._mp_manager.Coordinator(global_mgr_port=global_mgr_port, orchestration_port=orchestration_port)
    mgr.register_coordinator(host=host, coordinator=coordinator)
    return coordinator


# register the proxy class as the Coordinator class for SyncManager
SyncManager.register("Coordinator", Coordinator, exposed=["start", "shutdown", "kill", "start_worker"])

if __name__ == "__main__":
    try:
        uri = sys.argv[1]
        _host, _global_mgr_port, _orchestration_port = uri.split(':')
        _global_mgr_port = int(_global_mgr_port)
        _orchestration_port = int(_orchestration_port)
        _coordinator = coordinator_main(
            orchestration_port=_orchestration_port, global_mgr_port=_global_mgr_port, host=_host
        )
        sys.stdout.write("\nSTARTED\n")
        sys.stdout.flush()
        # block until client sends test on stdin as signal to terminate:
        line = ""
        while not line:
            line = sys.stdin.readline()
            if not _coordinator._worker_procs:
                break
            time.sleep(1)
        _coordinator.shutdown(timeout=3)
    except Exception as _e:
        import traceback
        always_print(traceback.format_exc(), as_error=True)
        always_print(f"FAILED: Exception starting coordinator: {_e}", as_error=True)
        sys.stdout.write(f"FAILED: Exception starting coordinator: {_e}\n")
