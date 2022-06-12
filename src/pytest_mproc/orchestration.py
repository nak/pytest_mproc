import asyncio
import hashlib
import multiprocessing
import os
import signal
import sys
import time
from contextlib import suppress
from enum import Enum
from glob import glob
from multiprocessing import Semaphore
from multiprocessing import JoinableQueue
from multiprocessing.managers import SyncManager, RemoteError
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple

from pytest_mproc import get_auth_key, find_free_port, get_ip_addr, Settings, AsyncMPQueue
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.fixtures import Global
from pytest_mproc.remote.ssh import SSHClient, remote_root_context
from pytest_mproc.user_output import always_print, debug_print
from pytest_mproc.worker import WorkerSession


class Protocol(Enum):
    LOCAL = 'local'
    REMOTE = 'remote'
    SSH = 'ssh'
    DELEGATED = 'delegated'


class OrchestrationManagerBase(Global.Manager):

    class Value:
        def __init__(self, val):
            self._val = val

        def value(self):
            return self._val

    def __init__(self, host: str, port: int, uri: str):
        super().__init__(host, port)
        OrchestrationManagerBase.register("Queue", multiprocessing.Queue, exposed=["get", "put"])
        self._uri = uri
        self._port = port
        self._host = host
        # only for SSH protocol:
        self._coordinators: List[Coordinator] = []
        self._workers: Dict[str, WorkerSession] = {}
        self._is_server = False
        self._mproc_pid: Optional[int] = None
        self._remote_pid: Optional[int] = None
        self._finalize_sem = multiprocessing.Semaphore(0)
        self._test_q = JoinableQueue()
        self._results_q =JoinableQueue()
        self._finalize_sem = Semaphore(0)

    @property
    def uri(self):
        return self._uri

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
        Connect as a client to an existing server

        :param host: if host assignment deferred, use this
        """
        if self._host is not None and host is not None:
            raise ValueError("Cannot specify host on connect when one already assigned on construction")
        elif self._host is None and host is None:
            raise ValueError(f"No host set")
        elif self._host is None:
            self._host = host
            self._port = port
        OrchestrationManagerBase.register("JoinableQueue")
        OrchestrationManagerBase.register("Semaphore")
        OrchestrationManagerBase.register("join")
        OrchestrationManagerBase.register("finalize")
        OrchestrationManagerBase.register("register_coordinator")
        OrchestrationManagerBase.register("register_worker")
        OrchestrationManagerBase.register("count_raw")
        OrchestrationManagerBase.register("completed")
        # noinspection SpellCheckingInspection
        OrchestrationManagerBase.register("get_finalize_sem")
        OrchestrationManagerBase.register("get_test_queue_raw")
        OrchestrationManagerBase.register("get_results_queue_raw")
        host = self._host.split('@', maxsplit=1)[-1]
        self.connect()

    def do_start(self):
        """
        Start this instance as the main server
        """
        OrchestrationManagerBase.register("join", self._join)
        OrchestrationManagerBase.register("get_finalize_sem", self._get_finalize_sem)
        OrchestrationManagerBase.register("finalize", self._finalize)
        OrchestrationManagerBase.register("register_coordinator", self._register_coordinator)
        OrchestrationManagerBase.register("register_worker", self._register_worker)
        OrchestrationManagerBase.register("count_raw", self._count)
        OrchestrationManagerBase.register("completed", self._completed)
        # noinspection SpellCheckingInspection
        OrchestrationManagerBase.register("JoinableQueue", JoinableQueue,
                                exposed=["put", "get", "task_done", "join", "close", "get_nowait"])
        OrchestrationManagerBase.register("Semaphore", Semaphore, exposed=["acquire", "release"])
        OrchestrationManagerBase.register("get_test_queue_raw", self._get_test_queue)
        OrchestrationManagerBase.register("get_results_queue_raw", self._get_results_queue)
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
        for client in self._coordinators:  # empty if client
            with suppress(ConnectionRefusedError, RemoteError):
                client.shutdown(timeout=timeout)

    def shutdown(self) -> None:
        self.join(timeout=5)
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

    def _get_results_queue(self) -> JoinableQueue:
        return self._results_q

    # noinspection PyUnresolvedReferences
    def _register_coordinator(self, client: Coordinator):
        self._coordinators.append(client)

    def _register_worker(self, worker: Tuple[str, int]):
        ip_addr, pid = worker
        worker_key = f"{ip_addr}-{pid}"
        self._workers[worker_key] = worker

    def _count(self):
        return self.Value(len(self._workers))

    def _completed(self, host: str, index: int):
        del self._workers[f"{host}-{index}"]

    def _get_finalize_sem(self)-> multiprocessing.Semaphore():
        return self._finalize_sem

    def _get_test_queue(self):
        return self._test_q

    def _finalize(self):
        self._finalize_sem.release()

    # noinspection SpellCheckingInspection
    @staticmethod
    def launch(destination: str, port: int, project_name: str) -> Tuple[int, int]:
        host = destination.split('@')[-1]
        try:
            user, _ = destination.split('@', maxsplit=1)
        except ValueError:
            user = Settings.ssh_username
        sem = multiprocessing.Semaphore(0)
        mpmgr = SyncManager(authkey=get_auth_key())
        mpmgr.start()
        try:
            returns = mpmgr.dict()
            mproc = multiprocessing.Process(target=OrchestrationManager._main,
                                            args=(sem, host, port, user, returns, project_name))
            mproc.start()
            sem.acquire()
            mproc_pid = mproc.pid
            remote_proc_pid = returns.get('remote_proc_pid')
        finally:
            mpmgr.shutdown()
        del sem
        return mproc_pid, remote_proc_pid

    @staticmethod
    def _main(sem: multiprocessing.Semaphore, host: str, port: int, user: str, returns: Dict[str, int],
              project_name: str):
        asyncio.get_event_loop().run_until_complete(OrchestrationManager._main_server(
            sem, host, port, user, returns, project_name))

    @staticmethod
    async def _main_server(sem: multiprocessing.Semaphore, host: str, port: int,
                           user: str, returns: Dict[str, int], project_name: str):
        remote_proc = None
        try:
            ssh = SSHClient(host=host, username=user)
            root = Path(__file__).parent.parent
            files = glob(str(root / 'pytest_mproc' / '**' / '*.py'), recursive=True)
            async with remote_root_context(project_name=project_name, ssh_client=ssh, remote_root=None) \
                    as (tmpdir, venv_dir):
                remote_sys_executable = 'python{}.{}'.format(*sys.version_info)
                await ssh.execute_remote_cmd(remote_sys_executable, '-m', 'venv', 'venv',
                                             cwd=venv_dir)
                python = f"{str(venv_dir)}{os.sep}venv{os.sep}bin{os.sep}python3"
                # await ssh.execute_remote_cmd(python, '-m', 'pip', 'install', 'pytest')
                env = os.environ.copy()
                env['PYTHONPATH'] = f"{str(tmpdir)}:."
                env['AUTH_TOKEN_STDIN'] = '1'
                await ssh.mkdir(tmpdir / 'pytest_mproc')
                await ssh.mkdir(tmpdir / 'pytest_mproc' / 'remote')
                for f in files:
                    assert Path(f).exists()
                    await ssh.push(Path(f), tmpdir / Path(f).relative_to(root))
                debug_print(
                    f"Launching global manager: {python} {str(Path('pytest_mproc') / Path(__file__).name)} "
                    f"{host} {port}")
                remote_proc = await ssh.launch_remote_command(
                    python, str(tmpdir / Path('pytest_mproc') / Path(__file__).name),
                    host, str(port), project_name,
                    cwd=tmpdir,
                    env=env,
                    stderr=asyncio.subprocess.PIPE,
                    stdout=sys.stderr,
                    stdin=asyncio.subprocess.PIPE,
                    auth_key=get_auth_key()
                )
                import binascii
                remote_proc.stdin.write(binascii.b2a_hex(get_auth_key()) + b'\n')
                remote_proc.stdin.close()
                sem.release()
                returns.update({'remote_proc_id': remote_proc.pid})
                await remote_proc.wait()
        except Exception:
            if remote_proc:
                remote_proc.terminate()
            sem.release()
            returns.update({'remote_proc_id': -1})
            raise


class OrchestrationManager:

    def __init__(self, *args, **kwargs):
        self._mp_manager = OrchestrationManagerBase(*args, **kwargs)

    @property
    def uri(self):
        return self._mp_manager.uri

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
    def create(uri: str, as_client: bool, project_name: Optional[str] = None, connect: bool = True)\
            -> "OrchestrationManager":
        """
        create an orchestration manager to manage test & results q's as well as clients/workers
        one of three protocols can be used in uri: local for running a server local,
        remote for connecting to a remote server already running, os ssh (in which case host can be
        in form of a destination: user@host).  In the latter it will launch a remote instance
        to the specified host of he server.  Note that running locally means that a port
        must be opened for incoming requests.  Firewalls can prevent this, hence the other
        protocol options

        :param project_name:  name of project under test
        :param uri: URI in form <protocol>://host:port where protocol is one of 'local', 'remote', 'ssh'
        :param as_client: only for local protocol: whether to instantiate as a server or as a client
        :return: OrchestrationManager instance
        """
        parts = uri.split('://', maxsplit=1)
        try:
            if uri.startswith('delegated://'):
                host = None
                port_text = uri.rsplit('://', maxsplit=1)[-1]
                if '@' in port_text:
                    username, port_text = port_text.split('@', maxsplit=1)
                protocol = Protocol.DELEGATED
            elif len(parts) == 1:
                host, port_text = parts[0].split(':', maxsplit=1)
                protocol = Protocol.LOCAL
            else:
                protocol_text = parts[0]
                host, port_text = parts[1].split(':', maxsplit=1)
                protocol = Protocol.LOCAL if as_client else Protocol(protocol_text)
            port = int(port_text) if port_text else (find_free_port() if protocol != Protocol.DELEGATED else -1)
        except ValueError:
            raise ValueError(f"URI {uri}: unknown protocol or invalid port specification")
        if host is not None and '@' in host:
            username, host = host.split('@', maxsplit=1)
        if protocol == Protocol.LOCAL:
            mgr = OrchestrationManager(host, port, uri=uri)
            if as_client:
                if connect:
                    mgr.connect()
            else:
                mgr.start()
                Global.Manager._singleton[get_auth_key()] = mgr
        elif protocol == Protocol.REMOTE:
            # always instantiate as client as there is an external server
            mgr = OrchestrationManager(host, port, uri=uri)
            mgr.connect()
        elif protocol == Protocol.SSH:
            assert project_name is not None
            mgr = OrchestrationManager(host, port, uri=uri)
            mgr._mproc_pid, mgr._remote_pid = mgr.launch(mgr.host, mgr.port, project_name)
            time.sleep(3)
            mgr.connect()
        elif protocol == Protocol.DELEGATED:
            mgr = OrchestrationManager(host, port, uri=uri)
            # will connect later as server is delegated to one of the workers and must wait for that to come up
        else:
            raise SystemError("Invalid protocol to process")
        return mgr

    def connect(self, host: Optional[str] = None, port: Optional[int] = None):
        self._mp_manager.do_connect(host, port)

    def start(self):
        self._mp_manager.do_start()

    def get_test_queue(self) -> AsyncMPQueue:
        return AsyncMPQueue(self._mp_manager.get_test_queue_raw())

    def get_results_queue(self) -> AsyncMPQueue:
        return AsyncMPQueue(self._mp_manager.get_results_queue_raw())

    def count(self):
        # noinspection PyUnresolvedReferences
        return self._mp_manager.count_raw().value()

    def get_finalize_sem(self):
        return self._mp_manager.get_finalize_sem()

    def completed(self, host: str, index: int):
        return self._mp_manager.completed(host, index)

    def register_coordinator(self, coordinator: Coordinator):
        return self._mp_manager.register_coordinator(coordinator)

    def register_worker(self, worker: Tuple[str, int]):
        return self._mp_manager.register_worker(worker)

    def join(self, timeout: Optional[float] = None):
        return self._mp_manager.join(timeout)

    def finalize(self):
        with suppress(ConnectionResetError, EOFError, RemoteError):  # other side will exit its process causing an error
            return self._mp_manager.finalize()

    def shutdown(self):
        return self._mp_manager.shutdown()


def main(host: str, port: int, project_name: str):
    if port == -1:
        port = find_free_port()
    sys.stdout.write(str(port) + '\n')
    sys.stdout.flush()
    always_print(f"Serving orchestration manager on {host}:{port} until released...")
    mgr = OrchestrationManager.create(uri=f"{host}:{port}", project_name=project_name,
                                      as_client=False)
    try:
        sem = mgr.get_finalize_sem()
        always_print(f"Waiting on finalize sem... {sem}")
        sem.acquire()
    finally:
        always_print("Shutting down delegated manager")
        mgr.shutdown()
    return mgr


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    project_name = sys.argv[3]
    main(host, port, project_name)
