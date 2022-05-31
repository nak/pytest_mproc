import asyncio
import hashlib
import multiprocessing
import os
import sys
import time
from contextlib import suppress
from enum import Enum
from glob import glob
from multiprocessing import Semaphore
from multiprocessing import JoinableQueue
from multiprocessing.process import current_process
from pathlib import Path
from typing import Optional, Dict, List, Any

from pytest_mproc import user_output
from pytest_mproc.fixtures import Global
from pytest_mproc.remote.ssh import SSHClient
from pytest_mproc.user_output import always_print


class Protocol(Enum):
    LOCAL = 'local'
    REMOTE = 'remote'
    SSH = 'ssh'


class OrchestrationManager:

    class Value:
        def __init__(self, val):
            self._val = val

        def value(self):
            return self._val

    def __init__(self, host: str, port: int):
        self._port = port
        self._host = host
        self._test_q: Dict[str, JoinableQueue] = {}
        self._results_q: Dict[str, JoinableQueue] = {}
        self._global_mgr = None
        # only for SSH protocol:
        self._remote_proc: Optional[asyncio.subprocess.Process] = None
        self._clients: Dict[str, List[Any]] = {}
        self._workers: Dict[str, Dict[str, Any]] = {}
        self._is_server = False
        self._mproc: Optional[multiprocessing.Process] = None

    @staticmethod
    def key() -> str:
        return hashlib.sha256(current_process().authkey).hexdigest()

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @staticmethod
    def create(uri: str, as_client: bool, serve_forever: bool = False) -> "OrchestrationManager":
        """
        create an orchestration manager to manage test & results q's as well as clients/workers
        one of three protocols can be used in uri: local for running a server local,
        remote for connecting to a remote server already running, os ssh (in which case host can be
        in form of a destination: user@host).  In the latter it will launch a remote instance
        to the specified host of he server.  Note that running locally means that a port
        must be opened for incoming requests.  Firewalls can prevent this, hence the other
        protocol options

        :param uri: URI in form <protocol>://host:port where protocol is one of 'local', 'remote', 'ssh'
        :param as_client: only for local protocol: whether to instantiate as a server or as a client
        :return: OrchestrationMenager instance
        """
        parts = uri.split('://', maxsplit=1)
        try:
            if len(parts) == 1:
                host, port_text = parts[0].split(':', maxsplit=1)
                protocol = Protocol.LOCAL
            else:
                protocol_text = parts[0]
                host, port_text = parts[1].split(':', maxsplit=1)
                protocol = Protocol(protocol_text)
            port = int(port_text)
        except ValueError:
            raise ValueError(f"URI {uri}: unknown protocol or invalid port specification")
        if protocol == Protocol.LOCAL:
            mgr = OrchestrationManager(host, port)
            if as_client:
                mgr.connect()
            else:
                mgr.start(serve_forever=serve_forever)
        elif protocol == Protocol.REMOTE:
            # always instantiate as client as there is an external server
            mgr = OrchestrationManager(host, port)
            mgr.connect()
        elif protocol == Protocol.SSH:
            mgr = OrchestrationManager(host, port)
            mgr.launch()
            # wait for server to start listening
            time.sleep(3)
            mgr.connect()
        else:
            raise SystemError("Invalid protocol to process")
        return mgr

    def connect(self) -> None:
        """
        Connect as a client to an existing server
        """
        Global.Manager.register("register_client")
        Global.Manager.register("register_worker")
        Global.Manager.register("count")
        Global.Manager.register("completed")
        Global.Manager.register("JoinableQueue")
        Global.Manager.register("get_test_queue")
        Global.Manager.register("get_results_queue")
        always_print(f"Connecting on {self._host} at {self._port} as client")
        self._global_mgr = Global.Manager.singleton(address=(self._host, self._port), as_client=True)

    def start(self, serve_forever: bool = False):
        """
        Start this instance as the main server
        """
        Global.Manager.register("register_client", self._register_client)
        Global.Manager.register("register_worker", self._register_worker)
        Global.Manager.register("count", self._count)
        Global.Manager.register("completed", self._completed)
        Global.Manager.register("JoinableQueue", JoinableQueue,
                                exposed=["put", "get", "task_done", "join", "close", "get_nowait"])
        Global.Manager.register("Semaphore", Semaphore, exposed=["acquire", "release"])
        Global.Manager.register("get_test_queue", self._get_test_queue)
        Global.Manager.register("get_results_queue", self._get_results_queue)
        # we only need these as dicts when running a standalone remote server handling multiple
        # pytest sessions/users
        self._global_mgr = Global.Manager.singleton(address=(self._host, self._port),
                                                    as_client=False, serve_forever=serve_forever)
        self._is_server = True

    def _join(self, key: str,  timeout: Optional[float] = None) -> None:
        """
        For server, join all existing clients, presumably when expecting client to complete soon
        For client, do nothing

        :param timeout: Optional timeout to wait
        :raises: TimeoutError if timeout is specified and reached before join completes
        """
        for client in self._clients[key]:  # empty if client
            client.join(timeout=timeout)

    def register_client(self, client):
        """
        Register a client with this manager, to be joined and cleaned up at end of test run
        :param client: client to register
        """
        # noinspection PyUnresolvedReferences
        self._global_mgr.register_client(client, self.key())

    def register_worker(self, worker) -> None:
        """
        Register worker (there can be multiple workers per client)
        :param worker: worker to regisster
        """
        # noinspection PyUnresolvedReferences
        self._global_mgr.register_worker(worker, self.key())

    def count(self):
        # noinspection PyUnresolvedReferences
        return self._global_mgr.count(self.key()).value()

    def completed(self, host: str, index: int):
        # noinspection PyUnresolvedReferences
        return self._global_mgr.completed(host, index, self.key())

    def get_test_queue(self):
        # noinspection PyUnresolvedReferences
        return self._global_mgr.get_test_queue(self.key())

    def get_results_queue(self):
        # noinspection PyUnresolvedReferences
        return self._global_mgr.get_results_queue(self.key())

    def shutdown(self):
        if self._remote_proc is not None:
            with suppress(Exception):
                self._remote_proc.terminate()
            with suppress(Exception):
                self._remote_proc.kill()
        if self._mproc is not None:
            with suppress(Exception):
                self._mproc.terminate()
            with suppress(Exception):
                self._mproc.kill()
        if self._is_server:
            self._global_mgr.shutdown()

    # server interface mapped to multiprocessing functions without the underscore in Global.Manager carried by this
    # instance
    def _get_test_queue(self, key: str):
        self._test_q.setdefault(key, JoinableQueue())
        return self._test_q[key]

    def _get_results_queue(self, key: str):
        self._results_q.setdefault(key, JoinableQueue())
        return self._results_q[key]

    # noinspection PyUnresolvedReferences
    def _register_client(self, client: "pytest_mproc.coordinator.Coordinator", key: str):
        self._clients.setdefault(key, []).append(client)

    def _register_worker(self, worker, key: str):
        ip_addr, pid = worker
        worker_key = f"{ip_addr}-{pid}"
        if key in self._workers and worker_key in self._workers[key]:
            raise Exception(f"Duplicate worker index: {worker} @  {worker_key}")
        self._workers.setdefault(key, {})[worker_key] = worker

    def _count(self, key: str):
        if key not in self._workers:
            return self.Value(0)
        return self.Value(len(self._workers[key]))

    def _completed(self, host: str, index: int, key: str):
        if key in self._workers:
            del self._workers[key][f"{host}-{index}"]
            if not self._workers[key]:
                del self._workers[key]

    def launch(self) -> None:
        host = self._host.split('@')[-1]
        port = self._port
        try:
            user, _ = self._host.split('@', maxsplit=1)
        except ValueError:
            user = None
        sem = multiprocessing.Semaphore(0)

        async def main_server(sem: multiprocessing.Semaphore):
            try:
                ssh = SSHClient(host=host, username=user)
                root = Path(__file__).parent.parent
                files = glob(str(root / '**' / '*.py'), recursive=True)
                async with ssh.mkdtemp() as tmpdir:
                    remote_sys_executable = 'python{}.{}'.format(*sys.version_info)
                    await ssh.execute_remote_cmd(remote_sys_executable, '-m', 'venv', 'venv',
                                                 cwd=tmpdir)
                    python = f"{str(tmpdir)}{os.sep}venv{os.sep}bin{os.sep}python3"
                    await ssh.execute_remote_cmd(python, '-m', 'pip', 'install', 'pytest')
                    env = os.environ.copy()
                    env['PYTHONPATH'] = f"{str(tmpdir)}:."
                    env['AUTH_TOKEN_STDIN'] = '1'
                    await ssh.mkdir(tmpdir / 'pytest_mproc')
                    await ssh.mkdir(tmpdir / 'pytest_mproc' / 'remote')
                    for f in files:
                        assert Path(f).exists()
                        await ssh.push(Path(f), tmpdir / Path(f).relative_to(root))
                    always_print(
                        f"Launching global manager: {python} {str(Path('pytest_mproc') / Path(__file__).name)} "
                        f"{host} {port}")
                    proc = await ssh.launch_remote_command(
                        python, str(tmpdir / Path('pytest_mproc') / Path(__file__).name),
                        host, str(port),
                        cwd=tmpdir,
                        env=env,
                        stderr=asyncio.subprocess.PIPE,
                        stdout=sys.stderr,
                        stdin=asyncio.subprocess.PIPE,
                        auth_key=current_process().authkey
                    )
                    import binascii
                    proc.stdin.write(binascii.b2a_hex(current_process().authkey) + b'\n')
                    proc.stdin.close()
                    self._remote_proc = proc
                    sem.release()
                    await proc.wait()
            except:
                if self._remote_proc:
                    self._remote_proc.terminate()
                sem.release()
                raise

        def main(sem: multiprocessing.Semaphore()):
            asyncio.get_event_loop().run_until_complete(main_server(sem))

        self._mproc = multiprocessing.Process(target=main, args=(sem, ))
        self._mproc.start()
        sem.acquire()


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    user_output.set_verbose(True)
    always_print(f"Serving orchestration manager on {host}:{port} forever...")
    mgr = OrchestrationManager.create(uri=f"{host}:{port}", as_client=False, serve_forever=True)
