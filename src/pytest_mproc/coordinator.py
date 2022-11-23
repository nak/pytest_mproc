"""
This package contains code to coordinate execution of workers on a host (either localhost or remote)
"""
import binascii
import multiprocessing
import os
import subprocess
import sys
import time
from configparser import ConfigParser
from dataclasses import dataclass
from multiprocessing import JoinableQueue
from multiprocessing.managers import BaseManager

from pathlib import Path
from typing import Optional, List, Dict, Tuple


from pytest_mproc import user_output, _find_free_port, worker
from pytest_mproc.auth import is_local_auth, ini_path

from pytest_mproc.user_output import always_print
from pytest_mproc.fixtures import Global, Node
from pytest_mproc.utils import BasicReporter


DEFAULT_MGR_PORT = 9893


@dataclass
class Value:
    """
    needed to allow in/out/return values for multiprocessing
    """
    value: int


class Coordinator(BaseManager):
    """
    Context manager for kicking off worker Processes to conduct test execution via pytest hooks
    Coordinators are scoped to a node and only handle a collection of workers on that node
    """

    _singleton: Optional["Coordinator"] = None
    _workers_by_session: Dict[str, Tuple[multiprocessing.Process, multiprocessing.Process]] = {}

    def __init__(self, address: Optional[Tuple[str, int]] = None, auth_key: Optional[bytes] = None):
        """
        """
        if address is not None:
            if auth_key is None:
                raise RuntimeError("Asked to start coordinator on address without auth token")
            if is_local_auth():
                raise RuntimeError(f"No ini file used to define auth token needed for distributed processing")
            super().__init__(address=address, authkey=auth_key)
        self._count = 0
        self._session_start_time = time.time()
        self._reporter = BasicReporter()
        self._worker_procs: List[Tuple[subprocess.Popen, subprocess.Popen]] = []
        self._remote_run_path: Optional[Path] = None
        self._node_mgr_port = _find_free_port()
        self._node_mgr = Node.Manager.singleton(self._node_mgr_port)
        self._host_sessions: List[HostSession] = []

    @classmethod
    def as_client(cls, address: Tuple[str, int], auth_token: bytes):
        """
        create Coordinator from a main process as a client to a worker host/node
        """
        instance = Coordinator(address=address, auth_key=auth_token)
        instance.connect()
        return instance

    @classmethod
    def as_server(cls, address: Tuple[str, int], auth_token: bytes) -> "Coordinator":
        """
        create coordinator on worker-host machine as server
        """
        instance = Coordinator(address=address, auth_key=auth_token)
        instance.start()
        return instance

    @classmethod
    def as_local(cls):
        """
        run only as localhost coordinator, without need for multiprocessing proxy-system, just
        direct API calls
        """
        return Coordinator()

    def create_host_session(
        self,
        num_processes: int,
        session_id: str,
        working_dir: Path,
        artifacts_rel_path: Path,
        global_mgr_host: str,
        global_mgr_port: int,
        test_q: JoinableQueue,
        results_q: JoinableQueue,
        addl_env: Optional[Dict[str, str]] = None,
        args: Optional[List[str]] = None,
    ) -> "HostSession":
        """
        State a worker

        :param num_processes: number of worker processes to launch
        :param session_id: unique session id (across all hosts for a single test session)
        :param working_dir: working path where pytest will execute
        :param artifacts_rel_path: relative path (to working dir) where artifacts will be placed
        :param global_mgr_host: host name/ip of server hosting global fixture information
        :param global_mgr_port: port of global mgr server
        :param test_q: queue to pull tests from
        :param results_q: queue to put test results/exceptions in
        :param addl_env: optional additional environ variable for worker processes
        :param args: optional additional args to pass to worker processes
        """
        host_session = HostSession(session_id=session_id,
                                   working_dir=working_dir, artifacts_rel_path=artifacts_rel_path,
                                   test_q=test_q, results_q=results_q,
                                   global_mgr_host=global_mgr_host,
                                   global_mgr_port=global_mgr_port
                                   )
        self._host_sessions.append(host_session)
        for index in range(num_processes):
            host_session.start_worker(args=args, addl_env=addl_env)
        return host_session


class HostSession:

    def __init__(self, session_id: str, global_mgr_host: str, global_mgr_port: int,
                 working_dir: Path, artifacts_rel_path: Path,
                 test_q: JoinableQueue, results_q: JoinableQueue):
        self._session_id = session_id
        self._working_dir = working_dir
        self._artifacts_rel_path = artifacts_rel_path
        self._test_q = test_q
        self._results_q = results_q
        self._node_mgr_port = _find_free_port()
        # for handling global fixtures:
        Global.Manager.singleton(address=(global_mgr_host, global_mgr_port), as_client=True)
        # TODO is host is 'localhost' ensure host code reverse-port-forwards port

    _worker_procs_by_session_id: Dict[str, List[multiprocessing.Process]] = []

    def start_worker(
            self,
            args: List[str],
            addl_env: Dict[str, str],
    ) -> None:
        """
        :param args: args to append to pytest process of worker
        :param addl_env: additional environment variables when launching worker
        :return: HostSession instance
        """
        worker_id = f"Worker-{self._session_id}-{len(HostSession._worker_procs_by_session_id[self._session_id]) + 1}"
        always_print(f"Starting worker with id {worker_id}")
        env = os.environ.copy()
        os.environ.update(addl_env)
        os.environ.update({
            "PTMPROC_WORKER": "1",
            'PTMPROC_VERBOSE': '1' if user_output.is_verbose else '0',
        })
        (self._working_dir / self._artifacts_rel_path).mkdir(exist_ok=True, parents=True)
        log_dir = self._working_dir
        Node.Manager.singleton(self._node_mgr_port)  # forces creation on defined port
        proc = multiprocessing.Process(target=self._launch_pytest,
                                       args=(args, worker_id, log_dir, env))
        proc.start()
        os.environ = env
        HostSession._worker_procs_by_session_id[self._session_id].append(proc)

    def shutdown(self, timeout: Optional[float] = None) -> None:
        """
        shutdown servers and clean up

        :param timeout: timeout if taking too long
        :raises: TimeoutError if taking too long
        """
        if hasattr(self, "_tee_proc"):
            self._tee_proc.kill()
        if self._session_id not in HostSession._worker_procs_by_session_id:
            return
        for worker_proc in HostSession._worker_procs_by_session_id[self._session_id]:
            worker_proc.join(timeout=timeout)
            if worker_proc.exitcode is None:
                worker_proc.terminate()
        del HostSession._worker_procs_by_session_id[self._session_id]

    def kill(self) -> None:
        """
        Abruptly kill all worker processes
        """
        if self._session_id in HostSession._worker_procs_by_session_id:
            for worker_proc in HostSession._worker_procs_by_session_id[self._session_id]:
                worker_proc.kill()
            del self._worker_procs_by_session_id[self._session_id]
        if hasattr(self, "_tee_proc"):
            self._tee_proc.kill()

    def wait(self, timeout: Optional[float] = None):
        for worker_proc in HostSession._worker_procs_by_session_id.get(self._session_id, []):
            worker_proc.join(timeout=timeout)
            if worker_proc.exitcode is None:
                worker_proc.terminate()

    def _launch_pytest(self, args: List[str],
                       worker_id: str, log_dir: Path):
        # this should be launched in a new mp Process
        Node.Manager.singleton(self._node_mgr_port)
        log = log_dir / 'pytest_run.log'
        stdout = subprocess.DEVNULL if not user_output.is_verbose else sys.stderr
        self._tee_proc = subprocess.Popen(
            ['tee', log],
            stdout=stdout,
            stderr=sys.stderr,
            bufsize=0,
            stdin=subprocess.PIPE
        )
        sys.stdout = self._tee_proc.stdin
        sys.stderr = self._tee_proc.stdin
        worker.main(test_q=self._test_q, results_q=self._results_q, args=args, worker_id=worker_id)
        always_print(f"Launched pytest worker")


def coordinator_main(host: str, port: int, auth_key: str) -> Coordinator:
    """
    entry point for running as main
    """
    return Coordinator.as_server(address=(host, port), auth_token=binascii.a2b_hex(auth_key))


if __name__ == "__main__":
    try:
        if ini_path() is not None:
            print(f"Ignoring cmd line and reading config from {ini_path()}")
            config_parser = ConfigParser()
            config_parser.read(ini_path())
            _host = config_parser.get(section='pytest_mproc', option="coordinator_host", fallback='localhost')
            _port = config_parser.get(section='pytest_mproc', option="coordinator_port", fallback=DEFAULT_MGR_PORT)
            _auth_key = config_parser.get(section='pytest_mproc', option='auth_key', fallback=None)
            if _port is None:
                print(f"No port defined for coordinator bring up in {ini_path()}")
                sys.exit(-1)
            if _auth_key is None:
                print(f"No auth key defined for coordinator bring up in {ini_path()}")
                sys.exit(-1)
            _global_mgr_port = int(_port)
        else:
            if len(sys.argv) > 2:
                print(f"Usage: {sys.argv[0]} [host:port]")
                sys.exit(-2)
            if len(sys.argv) == 2:
                uri = sys.argv[1]
                try:
                    _host, _global_mgr_port, _orchestration_port = uri.split(':')
                except ValueError:
                    print(f"Usage: {sys.argv[0]} [host:port]")
                    sys.exit(-2)
            else:
                print(f"No host/port pair specified.  Using port {DEFAULT_MGR_PORT} "
                      "and assuming localhost using ssh-reverse-port-forwarding as needed")
                _host = 'localhost'
                _global_mgr_port = DEFAULT_MGR_PORT

            print("No ini file, reading auth token from stdin (hex format)...")
            _auth_key = sys.stdin.readline()
            _auth_key = binascii.a2b_hex(_auth_key)
        _global_mgr_port = int(_global_mgr_port)
        sys.stdout.write("\nCoordinator STARTED\n")
        sys.stdout.flush()
        coordinator_main(_host, _global_mgr_port, _auth_key)
        sys.stdout.write("\nCoordinator FINISHED")
    except Exception as _e:
        import traceback
        always_print(traceback.format_exc(), as_error=True)
        always_print(f"FAILED: Exception starting coordinator: {_e}", as_error=True)
        sys.stdout.write(f"FAILED: Exception starting coordinator: {_e}\n")
