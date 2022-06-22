import asyncio
import multiprocessing
import os
import signal
import subprocess
import sys
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Optional, Dict, List, AsyncContextManager

from pytest_mproc.constants import ARTIFACTS_ZIP
from pytest_mproc.coordinator import Coordinator

from pytest_mproc import get_auth_key, Settings, AsyncMPQueue
from pytest_mproc.data import ClientDied
from pytest_mproc.ptmproc_data import ProjectConfig, RemoteWorkerConfig
from pytest_mproc.remote.bundle import Bundle
from pytest_mproc.remote.ssh import SSHClient, remote_root_context
from pytest_mproc.user_output import always_print, debug_print


POLLING_INTERVAL_HOST_VALIDATION = 1


class RemoteSessionManager:
    """
    class to conduct a remote session with multiple remote worker nodes
    """

    def __init__(self,
                 project_config: ProjectConfig,
                 remote_sys_executable: Optional[str],
                 ):
        """
        :param project_config: user defined project parameters needed for creating the bundle to send
        :param remote_sys_executable: optional string path to remote executable for Python to use (must version-match)
        """
        super().__init__()
        self._project_config = project_config
        self._remote_sys_executable = remote_sys_executable
        self._exc_manager = multiprocessing.managers.SyncManager(authkey=get_auth_key())
        self._exc_manager.register("Queue", multiprocessing.Queue, exposed=["get", "put", "empty"])
        self._validate_task = None
        self._session_task: Optional[asyncio.Task] = None
        self._pids: Dict[str, List[int]] = {}
        self._contexts: Dict[str, AsyncContextManager] = {}
        self._project_name = project_config.project_name
        self._tmp_path = tempfile.TemporaryDirectory()
        self._bundle = Bundle.create(root_dir=Path(self._tmp_path.name),
                                     project_config=self._project_config,
                                     system_executable=self._remote_sys_executable)
        self._deployment_tasks: Dict[str, asyncio.Task] = {}
        self._worker_tasks: List[asyncio.Task] = []
        self._worker_pids: Dict[str, List[int]] = {}
        self._tmp_path.__enter__()
        self._bundle.__enter__()
        self._remote_roots: Dict[str, Path] = {}

    def shutdown(self):
        for host, remote_root in self._remote_roots.items():
            with suppress(Exception):
                ssh_client = SSHClient(username=Settings.ssh_username, password=Settings.ssh_password, host=host)
                artifacts_zip = remote_root / ARTIFACTS_ZIP
                always_print(f"\n>> Pulling {artifacts_zip} from {host}\n")
                ssh_client.pull_sync(artifacts_zip, Path('.') / ARTIFACTS_ZIP)
                always_print(f"\n>> Unzipping {ARTIFACTS_ZIP}...\n")
                proc = subprocess.run(
                    f"unzip -o ./{ARTIFACTS_ZIP}",
                    shell=True,
                    stdout=sys.stdout,
                    stderr=sys.stderr
                )
                if proc.returncode == 0:
                    os.remove(ARTIFACTS_ZIP)
                else:
                    always_print(f"\n!! Failed to unzip {Path('.') / ARTIFACTS_ZIP}\n")
        with suppress(Exception):
            self._validate_task.cancel()
        for task in list(self._deployment_tasks.values()) + self._worker_tasks:
            with suppress(Exception):
                task.cancel()
        for host, remote_pids in self._worker_pids.items():
            for remote_pid in remote_pids:
                with suppress(Exception):
                    ssh_client = SSHClient(username=Settings.ssh_username, password=Settings.ssh_password, host=host)
                    ssh_client.signal_sync(remote_pid, signal.SIGKILL)
        self._worker_pids = {}

        async def cleanup():
            for context in self._contexts.values():
                await context.__aexit__(None, None, None)
        try:
            if asyncio.get_event_loop().is_running():
                asyncio.create_task(cleanup())
        except RuntimeError:
            pass
        with suppress(Exception):
            self._tmp_path.__exit__(None, None, None)
        with suppress(Exception):
            self._bundle.__exit__(None, None, None)

    async def setup_venv(self, worker_config: RemoteWorkerConfig):
        ssh_client = SSHClient(username=worker_config.ssh_username, password=Settings.ssh_password,
                               host=worker_config.remote_host)
        context = remote_root_context(self._project_name, ssh_client, worker_config.remote_root)
        self._contexts[worker_config.remote_host] = context
        (remote_root, remote_venv_root) = await context.__aenter__()
        remote_venv = remote_venv_root / 'venv'
        await self._bundle.setup_remote_venv(ssh_client=ssh_client, remote_venv=remote_venv)
        return remote_root, remote_venv

    async def _deploy(self, worker_config: RemoteWorkerConfig,
                      remote_root: Path, remote_venv: Path, timeout: Optional[float] = None,):
        host = worker_config.remote_host
        ssh_client = SSHClient(username=worker_config.ssh_username, password=Settings.ssh_password, host=host)
        await self._bundle.deploy(
            ssh_client=ssh_client,
            remote_root=remote_root,
            remote_venv=remote_venv,
            timeout=timeout
        )
        self._remote_roots[host] = remote_root

    async def start_worker(
            self,
            worker_config: RemoteWorkerConfig,
            coordinator: Coordinator,
            args: List[str],
            results_q: AsyncMPQueue,
            remote_root: Path,
            remote_venv: Path,
            deploy_timeout: Optional[float] = None,
            env: Dict[str, str] = None,
    ):
        if worker_config.remote_host not in self._deployment_tasks:
            self._deployment_tasks[worker_config.remote_host] = asyncio.create_task(
                self._deploy(worker_config, timeout=deploy_timeout, remote_root=remote_root, remote_venv=remote_venv)
            )

        async def start(index: int):
            try:
                await self._deployment_tasks[worker_config.remote_host]
                worker_pid = coordinator.start_worker(index=index, args=args, addl_env=env)
                self._worker_pids.setdefault(worker_config.remote_host, []).append(worker_pid)
                if not self._validate_task:
                    self._validate_task = asyncio.create_task(self.validate_clients(results_q=results_q))
            except Exception as e:
                import traceback
                always_print(traceback.format_exc())
                always_print(f"EXCEPTION in starting worker: {e}")
                raise

        self._worker_tasks.append(asyncio.create_task(start(len(self._worker_tasks) + 1)))

    async def validate_clients(self, results_q: AsyncMPQueue,) -> None:
        """
        Continually ping workers to ensure they are alive, and drop worker if not, rescheduling any pending tests to
        that worker

        :param results_q: Used to signal a client died
        """
        while self._worker_pids:
            for host in list(self._worker_pids.keys()):
                # test if worker is active
                try:
                    completed = subprocess.run(["ping", "-W",  "1", "-c", "1", host],
                                               stdout=subprocess.DEVNULL,
                                               timeout=5)
                    active = completed.returncode == 0
                except TimeoutError:
                    active = False
                # reschedule any tests in progress for tha worker if not
                if not active:
                    always_print(f"Host {host} unreachable!")
                    worker_pids = self._worker_pids.get(host)
                    ssh_client = SSHClient(username=Settings.ssh_username, password=Settings.ssh_password, host=host)
                    for worker_pid in worker_pids:
                        await ssh_client.signal(worker_pid, signal.SIGKILL)
                        # this will attempt to reschedule test
                        result = ClientDied(worker_pid, host, True)
                        await results_q.put(result)
                        self._worker_pids[host].remove(worker_pid)
                        if not self._worker_pids[host]:
                            del self._worker_pids[host]
                else:
                    debug_print(f"{host} is alive")
            await asyncio.sleep(POLLING_INTERVAL_HOST_VALIDATION)
