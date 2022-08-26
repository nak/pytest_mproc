import asyncio
import binascii
import multiprocessing
import os
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
from multiprocessing import JoinableQueue
from pathlib import Path
from typing import Optional, Dict, List

from pytest_mproc.constants import ARTIFACTS_ZIP
from pytest_mproc.coordinator import Coordinator

from pytest_mproc import get_auth_key, Settings, AsyncMPQueue, user_output
from pytest_mproc.data import ClientDied
from pytest_mproc.ptmproc_data import ProjectConfig, RemoteWorkerConfig
from pytest_mproc.remote.bundle import Bundle
from pytest_mproc.remote.ssh import SSHClient, remote_root_context
from pytest_mproc.user_output import always_print, debug_print


POLLING_INTERVAL_HOST_VALIDATION = 5


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
        self._site_pkgs_path: Dict[str, Path] = {}
        self._coordinator_sems: Dict[str, asyncio.Semaphore] = {}
        self._coordinator_tasks: Dict[str, asyncio.Task] = {}
        self._coordinator_procs: Dict[str, asyncio.subprocess.Process] = {}
        self._port_fwd_procs: List[asyncio.subprocess.Process] = []
        self._project_config = project_config
        self._remote_sys_executable = remote_sys_executable
        self._exc_manager = multiprocessing.managers.SyncManager(authkey=get_auth_key())
        self._exc_manager.register("Queue", multiprocessing.Queue, exposed=["get", "put", "empty"])
        self._validate_task = None
        self._session_task: Optional[asyncio.Task] = None
        self._pids: Dict[str, List[int]] = {}
        self._project_name = project_config.project_name
        self._tmp_path = tempfile.TemporaryDirectory()
        self._bundle = Bundle.create(root_dir=Path(self._tmp_path.name),
                                     project_config=self._project_config,
                                     system_executable=self._remote_sys_executable)
        self._worker_tasks: List[asyncio.Task] = []
        self._worker_pids: Dict[str, List[int]] = {}
        self._tmp_path.__enter__()
        self._bundle.__enter__()
        self._remote_roots: Dict[str, Path] = {}
        self._coordinators: Dict[str, Coordinator.ClientProxy] = {}
        self._ssh_clients: Dict[str, SSHClient] = {}

    def shutdown(self):
        for host, coordinator in self._coordinators.items():
            with suppress(Exception):
                try:
                    coordinator.shutdown(timeout=5)
                except asyncio.TimeoutError:
                    always_print(f"!!! Failed to stop coordinator on host {host}")
        self._coordinators = {}
        for host, remote_pids in self._worker_pids.items():
            always_print(f"Ensuring termination of remote worker pids {remote_pids} on {host}...")
            self._ssh_clients[host].signal_sync(remote_pids, sig_number=signal.SIGINT)
        self._worker_pids = {}
        with suppress(Exception):
            self._validate_task.cancel()
        for proc in self._port_fwd_procs:
            with suppress(Exception):
                proc.terminate()
        for proc in self._coordinator_procs.values():
            with suppress(Exception):
                proc.kill()
        for task in self._worker_tasks:
            with suppress(Exception):
                task.cancel()
        self._worker_tasks = []
        for host, remote_root in self._remote_roots.items():
            with suppress(Exception):
                ssh_client = self._ssh_clients[host]
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
        self._remote_roots = {}
        with suppress(Exception):
            self._tmp_path.__exit__(None, None, None)
        with suppress(Exception):
            self._bundle.__exit__(None, None, None)

    async def _setup(self, worker_config: RemoteWorkerConfig,
                     remote_root: Path,
                     remote_venv: Path,
                     deploy_timeout: Optional[float] = None) -> None:
        ssh_client = SSHClient(username=worker_config.ssh_username or Settings.ssh_username,
                               password=Settings.ssh_password,
                               host=worker_config.remote_host)
        self._ssh_clients[worker_config.remote_host] = ssh_client

        futures, _ = await asyncio.wait(
            [self._bundle.setup_remote_venv(ssh_client=ssh_client, remote_venv=remote_venv),
             self._deploy(ssh_client, worker_config, timeout=deploy_timeout, remote_root=remote_root)],
            timeout=deploy_timeout,
            return_when=asyncio.ALL_COMPLETED
        )
        results = [f.result() for f in futures]
        for r in results:
            if isinstance(r, Exception):
                raise r
        await self._bundle.install_requirements(ssh_client, remote_root=remote_root, remote_venv=remote_venv)

    async def _deploy(self, ssh_client: SSHClient, worker_config: RemoteWorkerConfig,
                      remote_root: Path, timeout: Optional[float] = None,):
        host = worker_config.remote_host
        await self._bundle.deploy(
            ssh_client=ssh_client,
            remote_root=remote_root,
            timeout=timeout
        )
        self._remote_roots[host] = remote_root

    async def start_worker(
            self,
            artifacts_dir: Path,
            worker_config: RemoteWorkerConfig,
            coordinator_q: JoinableQueue,
            args: List[str],
            results_q: AsyncMPQueue,
            local_global_mgr_port: int,
            local_orchestration_port: int,
            env: Dict[str, str] = None,
    ) -> None:
        debug_print(f"Starting worker ove ssh on {worker_config.remote_host}")
        self._ssh_clients[worker_config.remote_host] = SSHClient(
            username=worker_config.ssh_username or Settings.ssh_username,
            password=Settings.ssh_password,
            host=worker_config.remote_host)
        await self._coordinator_sems.setdefault(worker_config.remote_host, asyncio.Semaphore(1)).acquire()
        if worker_config.remote_host not in self._coordinator_tasks:
            await self._launch_coordinator(worker_config,
                                           project_name=self._project_config.project_name,
                                           artifacts_dir=artifacts_dir,
                                           local_global_mgr_port=local_global_mgr_port,
                                           local_orchestration_port=local_orchestration_port,
                                           coordinator_q=coordinator_q)
        self._coordinator_sems[worker_config.remote_host].release()
        assert worker_config.remote_host in self._coordinator_tasks

        async def start(index: int):
            try:
                coordinator = self._coordinators[worker_config.remote_host]
                env['PYTHONPATH'] = str(self._site_pkgs_path[worker_config.remote_host])
                debug_print(f"Calling coordinator to start worker {index} ...")
                worker_pid = coordinator.start_worker(index=index, args=args, addl_env=env)
                debug_print(f"Coordinator started worker @ {index} worke pid is {worker_pid}")
                self._worker_pids.setdefault(worker_config.remote_host, []).append(worker_pid)
                if not self._validate_task:
                    self._validate_task = asyncio.create_task(self.validate_clients(results_q=results_q))
            except Exception as e:
                import traceback
                always_print(traceback.format_exc(), as_error=True)
                always_print(f"EXCEPTION in starting worker: {e}", as_error=True)
                raise

        self._worker_tasks.append(asyncio.create_task(start(len(self._worker_tasks) + 1)))

    async def validate_clients(self, results_q: AsyncMPQueue,) -> None:
        """
        Continually ping workers to ensure they are alive, and drop worker if not, rescheduling any pending tests to
        that worker

        :param results_q: Used to signal a client died
        """
        while self._coordinators:
            for host, coordinator in self._coordinators.copy().items():
                # test if worker is active
                ssh_client = self._ssh_clients[host]
                if await ssh_client.ping(timeout=5):
                    debug_print(f"Host {host} is alive...")
                else:
                    always_print(f"Host {host} unreachable!", as_error=True)
                    worker_pids = self._worker_pids.get(host) or []
                    ssh_client.signal_sync(worker_pids, signal.SIGINT)
                    for worker_pid in worker_pids:
                        # this will attempt to reschedule test
                        result = ClientDied(worker_pid, host, True)
                        await results_q.put(result)
                    del self._worker_pids[host]
            await asyncio.sleep(POLLING_INTERVAL_HOST_VALIDATION)

    @classmethod
    async def create_site_pkgs(cls, ssh_client: SSHClient, remote_root: Path) -> Path:
        root = Path(__file__).parent.parent
        site_pkgs_path = remote_root / 'site-packages'
        await ssh_client.mkdir(site_pkgs_path / 'pytest_mproc' / 'remote', exists_ok=True)
        await ssh_client.push(root / 'pytest_mproc' / '*.py', site_pkgs_path / 'pytest_mproc' / '.')
        await ssh_client.push(local_path=root / 'pytest_mproc' / 'remote' / '*.py',
                              remote_path=site_pkgs_path / 'pytest_mproc' / 'remote' / '.')
        return site_pkgs_path

    async def _launch_coordinator(
            self, config: RemoteWorkerConfig,
            project_name: str,
            local_orchestration_port: int,
            local_global_mgr_port: int,
            artifacts_dir: Path,
            coordinator_q: JoinableQueue,
    ) -> None:
        ssh_client = SSHClient(host=config.remote_host,
                               username=config.ssh_username or Settings.ssh_username,
                               password=Settings.ssh_password)
        remote_global_mgr_port = await ssh_client.find_free_port()
        remote_orch_mgr_port = await ssh_client.find_free_port()
        sem = asyncio.Semaphore(0)
        exc = None

        async def task():
            nonlocal exc
            port_fwd_proc1 = await ssh_client.reverse_port_forward(local_global_mgr_port, remote_global_mgr_port)
            port_fwd_proc2 = await ssh_client.reverse_port_forward(local_orchestration_port, remote_orch_mgr_port)
            self._port_fwd_procs += [port_fwd_proc1, port_fwd_proc2]
            with remote_root_context(
                project_name=project_name,
                ssh_client=ssh_client,
                remote_root=config.remote_root or Settings.tmp_root
            ) as (remote_root, remote_venv_root):
                try:
                    coordinator_index = len(self._coordinators) + 1
                    remote_venv = remote_venv_root / 'venv'
                    remote_sys_executable = str(remote_venv / 'bin' / 'python3')
                    await self._setup(worker_config=config, remote_root=remote_root, remote_venv=remote_venv)
                    site_pkgs_path = await self.create_site_pkgs(ssh_client, remote_root)
                    self._site_pkgs_path[config.remote_host] = site_pkgs_path
                    python_path = str(site_pkgs_path)
                    env = {'AUTH_TOKEN_STDIN': '1',
                           'PYTHONPATH': python_path}
                    proc = await ssh_client.launch_remote_command(
                        f"{remote_sys_executable} -m pytest_mproc.coordinator "
                        f"{config.remote_host}:{remote_global_mgr_port}:{remote_orch_mgr_port} {coordinator_index} "
                        f"{str(artifacts_dir)} {remote_root or ''} {remote_venv or ''}",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=sys.stderr,
                        stdin=asyncio.subprocess.PIPE,
                        shell=True,
                        env=env,
                        auth_key=get_auth_key()
                    )
                    proc.stdin.write(binascii.b2a_hex(get_auth_key()) + b'\n')
                    proc.stdin.close()
                    text = ""
                    start_time = time.monotonic()
                    while text != 'STARTED':
                        text = (await proc.stdout.readline()).decode('utf-8').strip()
                        if text.startswith("FAILED"):
                            raise SystemError(f"Failed to launch coordinator: {text}")
                        if user_output.is_verbose and text and text != "STARTED":
                            print(text.strip())
                        if time.monotonic() - start_time > 30:
                            raise SystemError(f"Coordinator on host {config.remote_host} did not start in time")
                    always_print(f"Coordinator launched successfully on {config.remote_host}")
                    self._coordinator_procs[config.remote_host] = proc
                    self._coordinators[config.remote_host] = Coordinator.ClientProxy(host=config.remote_host,
                                                                                     q=coordinator_q)
                except Exception as e:
                    exc = e
                sem.release()
                line = "True"
                # keep within context until proc is completed or killed, spewing out output if user requested:
                while line:
                    line = (await proc.stdout.readline()).decode('utf-8')
                    if user_output.is_verbose:
                        print(line)
                await asyncio.wait_for(proc.wait(), timeout=3)
                self.shutdown()
                with suppress(Exception):
                    proc.kill()

        if exc:
            raise exc
        self._coordinator_tasks[config.remote_host] = asyncio.create_task(task())
        await sem.acquire()
