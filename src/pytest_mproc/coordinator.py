"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""
import os
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from multiprocessing import JoinableQueue

from pathlib import Path
from subprocess import TimeoutExpired
from typing import Optional, List, Dict, Tuple

from pytest_mproc import user_output
from pytest_mproc.constants import ARTIFACTS_ZIP

from pytest_mproc.user_output import always_print, debug_print
from pytest_mproc.fixtures import Global
from pytest_mproc.worker import WorkerSession
from pytest_mproc.utils import BasicReporter


@dataclass
class Value:
    value: int


class Coordinator:
    """
    Context manager for kicking off worker Processes to conduct test execution via pytest hooks
    Coordinators are scoped to a node and only handle a collection of workers on that node
    """

    class ClientProxy:
        """
        proxy interface for distributed communications;

        NOTE: on either some machine or with VPN/firewall configurations
        registering the Coordinator class doesn't work (upon registering coordinator with the orchestrator, a
        ConnectionError is thrown from the orchestrator when processing the register call)
        """

        def __init__(self, q: JoinableQueue, host: str):
            self._q = q
            self._host = host

        @property
        def host(self):
            return self._host

        def kill(self):
            self._q.put(('kill',))

        def shutdown(self, timeout: Optional[float] = None):
            self._q.put(('shutdown', timeout))

        def start_workers(
            self, num_processes: int,
            addl_env: Optional[Dict[str, str]] = None,
            args: Optional[List[str]] = None
        ):
            self._q.put(('start_workers', num_processes, addl_env, args))

        def start_worker(self, index: int, args: List[str], addl_env: Dict[str, str]) -> int:
            self._q.put(('start_worker', index, args, addl_env))
            self._q.join()
            pid = self._q.get()
            self._q.task_done()
            return pid

        def count(self) -> int:
            self._q.put(('count',))
            self._q.join()
            count = self._q.get()
            self._q.task_done()
            return count

    class HostProxy:
        """
        Host Proxy to listen for incoming commands (see above discussion on ClientProxy as to why we need this)
        """

        def __init__(self, q: JoinableQueue, target: "Coordinator"):
            self._q = q
            self._target = target

        def loop(self):
            cmd = None
            try:
                while cmd not in ('shutdown', 'kill'):
                    cmd, *args = self._q.get()
                    self._q.task_done()
                    if cmd == 'kill':
                        self._target.kill()
                    elif cmd == 'shutdown':
                        self._target.shutdown(*args)
                        self._q.put(None)  # just a signal
                    elif cmd == 'start_workers':
                        self._target.start_workers(*args)
                    elif cmd == 'start_worker':
                        pid = self._target.start_worker(*args)
                        self._q.put(pid)
                    elif cmd == 'count':
                        count = self._target.count()
                        self._q.put(count)
            except (EOFError, KeyboardInterrupt, BrokenPipeError, ConnectionError):
                self._target.shutdown(timeout=5)

    _singleton: Optional["Coordinator"] = None

    def __init__(self, global_mgr_port: int, orchestration_port: int, coordinator_index: int,
                 artifacts_dir: Path, remote_root: Optional[Path] = None, remote_venv: Optional[Path] = None
                 ):
        """
        """
        assert Coordinator._singleton is None, "Attempt to instantiate Coordinator more than once on same machine"
        self._index = coordinator_index
        self._count = 0
        self._global_mgr_port = global_mgr_port
        self._orchestration_port = orchestration_port
        self._session_start_time = time.time()
        self._reporter = BasicReporter()
        self._worker_procs: List[Tuple[subprocess.Popen, subprocess.Popen]] = []
        self._remote_run_path: Optional[Path] = None
        self._remote_root = remote_root
        self._remote_venv = remote_venv
        self._artifacts_path = remote_root / artifacts_dir if remote_root else Path(os.getcwd()) / artifacts_dir
        self._artifacts_path.mkdir(exist_ok=True, parents=True)
        self._relative_artifacts_path = artifacts_dir
        if remote_root:
            remote_root = Path(os.getcwd())
            prepare_script = remote_root / 'prepare'
            if prepare_script.is_file():
                if not os.access(prepare_script, os.X_OK):
                    raise PermissionError(f"Prepare script is not executable")
                with open(self._artifacts_path / 'prepare.log', 'w') as out_stream, \
                     open(self._artifacts_path / 'prepare_errors.log', 'w') as error_stream:
                    proc = subprocess.run(
                        str(prepare_script.absolute()),
                        stdout=out_stream,
                        stderr=error_stream,
                        cwd=remote_root,
                        shell=True
                    )
                if proc.returncode != 0:
                    raise RuntimeError(f"Failed to execute prepare script, return code {proc.returncode}")

    def count(self) -> int:
        """
        :return: current count of workers
        """
        return len(self._worker_procs)

    def start_worker(self, index: int, args: List[str], addl_env: Dict[str, str]) -> int:
        """
        start a single worker based on config

        :param args: additional arguments to pass to worker proces
        :param addl_env: additional environment variables to set for worker process
        :param index: unique index of worker

        :return: pid of created process (must be pickleable, so don't return Popen instance!)
        """
        try:
            index = len(self._worker_procs) + 1
            root_path = self._remote_root if self._remote_root else Path(os.getcwd())
            root_path.mkdir(exist_ok=True, parents=True)
            proc, tee_proc = WorkerSession.start(
                index=(self._index, index),
                orchestration_port=self._orchestration_port,
                global_mgr_port=self._global_mgr_port,
                args=args,
                addl_env=addl_env,
                root_path=root_path,
                venv_path=self._remote_venv,
                worker_artifacts_dir=self._relative_artifacts_path,
                artifacts_root=self._artifacts_path
            )
            self._worker_procs.append((proc, tee_proc))
            if proc.returncode is not None:
                raise SystemError(f"Worker-{index} failed to start")
            return proc.pid
        except Exception as e:
            raise SystemError(f"Failed to launch Worker-{index}: {e}") from e

    def start_workers(self, num_processes: int,
                      addl_env: Optional[Dict[str, str]] = None,
                      args: Optional[List[str]] = None,
                      ) -> List[int]:
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
        for index, (proc, tee_proc) in enumerate(self._worker_procs):
            if proc.returncode is not None:
                always_print(f"Worker-{index} failed to start")
                self._worker_procs.remove((proc, tee_proc))
                pids.remove(pids[index])
                with suppress(Exception):
                    tee_proc.kill()
        if not self._worker_procs:
            raise SystemError("All workers failed to start")
        return pids

    def wait(self, timeout: Optional[float] = None):
        for proc, tee_proc in self._worker_procs:
            try:
                proc.wait(timeout=timeout)
            finally:
                proc.terminate()
                tee_proc.terminate()

    def shutdown(self, timeout: Optional[float] = None) -> None:
        """
        shutdown servers and clean up

        :param timeout: timeout if taking too long
        :raises: TimeoutError if taking too long
        """
        try:
            if self._remote_run_path is not None:
                try:
                    finalize_script = self._remote_root / 'finalize'
                    # if not artifacts_path, no prepare script was executed, so no need to invoke finalize
                    if self._artifacts_path and finalize_script.is_file():
                        if not os.access(finalize_script, os.X_OK):
                            raise PermissionError(f"Finalize script is not executable")
                        with open(self._artifacts_path / 'finalize.log', 'w') as out_stream, \
                             open(self._artifacts_path / 'finalize_errors.log', 'w') as error_stream:
                            proc = subprocess.run(
                                str(finalize_script.absolute()),
                                stdout=out_stream,
                                stderr=error_stream,
                                cwd=self._remote_root,
                                shell=True
                            )
                            if proc.returncode != 0:
                                raise RuntimeError(f"Failed to execute finalize script, return code {proc.returncode}")
                except RuntimeError:
                    raise
                except Exception as e:
                    always_print(f"ERROR!: Unexpected exception in finalize execution: {e}")
        finally:
            if self._remote_root and self._artifacts_path.exists():
                always_print(f"Zipping artifacts to {Path('.') / ARTIFACTS_ZIP} for transfer...")
                debug_print(f"Executing command  'zip -r {ARTIFACTS_ZIP} {str(self._relative_artifacts_path)}'")
                completed = subprocess.run(
                    f"zip -r {ARTIFACTS_ZIP} {str(self._relative_artifacts_path)}",
                    shell=True,
                    cwd=str(self._remote_root),
                    stdout=sys.stdout if user_output.is_verbose else subprocess.DEVNULL,
                    stderr=sys.stderr
                )
                if completed.returncode != 0:
                    always_print(f"Failed to zip {self._relative_artifacts_path} from {self._remote_root}",
                                 as_error=True)
            for proc, tee_proc in self._worker_procs:
                try:
                    proc.terminate()
                    proc.wait(timeout=timeout)
                except (TimeoutExpired, KeyboardInterrupt):
                    with suppress(OSError):
                        proc.kill()
                try:
                    tee_proc.terminate()
                    tee_proc.wait(timeout=timeout)
                except (TimeoutExpired, KeyboardInterrupt):
                    with suppress(OSError):
                        tee_proc.kill()
            self._worker_procs = []

    def kill(self) -> None:
        """
        Abruptly kill all worker processes
        """
        for proc, tee_proc in self._worker_procs:
            proc.kill()
            tee_proc.kill()
        self._worker_procs = []


def coordinator_main(global_mgr_port: int, orchestration_port: int, host: str, index: int,
                     artifacts_path: Path, remote_root: Optional[Path] = None,
                     remote_venv: Optional[Path] = None) -> Tuple[Coordinator.HostProxy, Coordinator]:
    """
    entry point for running as main
    """
    from pytest_mproc.orchestration import OrchestrationManager
    Global.Manager.singleton(address=('localhost', global_mgr_port), as_client=True)
    mgr = OrchestrationManager.create_client(address=('localhost', orchestration_port))
    coordinator = Coordinator(global_mgr_port=global_mgr_port, orchestration_port=orchestration_port,
                              coordinator_index=index, artifacts_dir=artifacts_path,
                              remote_root=remote_root, remote_venv=remote_venv)
    proxy = Coordinator.HostProxy(q=mgr.get_coordinator_q(host=host),
                                  target=coordinator)
    return proxy, coordinator


if __name__ == "__main__":
    try:
        uri = sys.argv[1]
        _index = int(sys.argv[2])
        _artifacts_path = Path(sys.argv[3])
        _remote_root = Path(sys.argv[4]) if (len(sys.argv) > 4) else None
        _remote_venv = Path(sys.argv[5]) if (len(sys.argv) > 5) else None
        _host, _global_mgr_port, _orchestration_port = uri.split(':')
        _global_mgr_port = int(_global_mgr_port)
        _orchestration_port = int(_orchestration_port)
        _proxy, _coordinator = coordinator_main(
            orchestration_port=_orchestration_port, global_mgr_port=_global_mgr_port, host=_host, index=_index,
            artifacts_path=_artifacts_path, remote_root=_remote_root, remote_venv=_remote_venv
        )
        sys.stdout.write("\nSTARTED\n")
        sys.stdout.flush()
        _proxy.loop()
        _coordinator.kill()
        always_print(f"Coordinator {_index} completed")
    except Exception as _e:
        import traceback
        always_print(traceback.format_exc(), as_error=True)
        always_print(f"FAILED: Exception starting coordinator: {_e}", as_error=True)
        sys.stdout.write(f"FAILED: Exception starting coordinator: {_e}\n")
