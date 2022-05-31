import json
import logging
import multiprocessing
import os
import shutil
import socket
import sys
import time
from multiprocessing.process import current_process

import pytest
import subprocess
from asyncio import Semaphore
from pathlib import Path

from pytest_mproc import find_free_port
from pytest_mproc.fixtures import Global
from pytest_mproc.main import RemoteExecutionThread
from pytest_mproc.orchestration import OrchestrationManager
from pytest_mproc.ptmproc_data import RemoteHostConfig, ProjectConfig

from pytest_mproc.remote.bundle import Bundle
from pytest_mproc.remote.ssh import SSHClient, CommandExecutionFailure

_base_dir = Path(__file__).parent.parent
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__file__)
ssh_pswdless = (Path.home() / ".ssh" / "authorized_keys").exists()


class TestSSHClient:

    @pytest.mark.asyncio
    async def test_mkdtmp(self):
        ssh_client = SSHClient(socket.gethostbyname(socket.gethostname()))
        async with ssh_client.mkdtemp() as tmpdir:
            assert Path(tmpdir).exists()  # this is localhost after all
        assert not Path(tmpdir).exists()


@pytest.fixture(scope='session')
def bundle(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp("remote_testing")
    with Bundle.create(
            root_dir=Path(str(tmpdir)),
            project_config=ProjectConfig(
                project_root=Path(__file__).parent.parent,
                src_paths=[Path("src"), Path("testsrc")],
                test_files=[Path("test/*.py")],
                project_name="pytest_mproc_test"
            ),
            system_executable=sys.executable
    ) as bundle:
        bundle.add_file(_base_dir / "test" / "run_test.sh", "scripts/run_test.sh")
        yield bundle


@pytest.mark.skipif(not ssh_pswdless, reason="No permission to ssh to localhost without password input")
@pytest.mark.asyncio
async def test_execute_bundle(bundle):
    lines = []
    try:
        async for line in bundle.monitor_remote_execution("localhost", ".", "-k", "alg1", timeout=100,
                                                          env=dict(os.environ)):
            lines.append(line)
            print(line)
    except CommandExecutionFailure:
        text = '\n'.join(lines)
        assert False, f"Execution failed:\n{text}"


@pytest.mark.asyncio
async def test_execute_remote_multi(bundle):
    hosts = [
        RemoteHostConfig("localhost", {"cores": "1"}),
        RemoteHostConfig("localhost"),
        RemoteHostConfig("localhost", {"cores": "1"}),
    ]
    sys.argv.extend(['--cores', '2'])
    sem = Semaphore(0)
    environ = {'PTMPROC_NODE_MGR_PORT': str(find_free_port()), 'PTMPROC_GLOBAL_MGR_PORT': str(find_free_port())}
    procs = await bundle.execute_remote_multi(hosts, sem, ".", "-k", "alg3", "--cores", "2",
                                              deploy_timeout=100, timeout=200,
                                              env=environ, auth_key=multiprocessing.current_process().authkey)
    for host, proc in procs.items():
        assert host == "localhost"
        assert proc.returncode != 0  # alg3 delibrately coded to fail


def test_remote_execution_cli(tmp_path):
    root = Path(__file__).parent.parent
    ipname = socket.gethostbyname(socket.gethostname())
    project_config = ProjectConfig(src_paths=[Path("src"), Path("testsrc")],
                                   test_files=[Path("test") / "*.py", Path("requirements.txt")],
                                   project_root=root,
                                   project_name="pytest_mproc_test"
                                   )
    project_config_path = tmp_path / "project.cfg"
    for path in project_config.src_paths + [Path("test")]:
        shutil.copytree(root / path, tmp_path / path.name)
    shutil.copy(root / "test" / "resources" / "requirements.txt", tmp_path / "requirements.txt")
    with open(project_config_path, 'w') as out:
        converted = {'src_paths': [str(p) for p in project_config.src_paths],
                     'test_files': [str(p) for p in project_config.test_files],
                     'project_name': project_config.project_name}
        out.write(json.dumps(converted))
        out.flush()
        # remote_host = 'ssh://pi@10.220.45.119'
        remote_host = '127.0.0.1'
        client_connect = remote_host.split("://")[-1]
        args = [
            'pytest', '-s', 'test/test_mproc_runs.py', '-k', 'alg2',
            '--cores', '3',
            '--as-main', f"{remote_host}:{find_free_port()}",
            '--project-structure', str(project_config_path),
            '--remote-worker', client_connect,
            '--remote-worker', client_connect,
            '--remote-worker', client_connect,
            '--remote-worker', client_connect,
            '--remote-worker', client_connect,
        ]
        sys.path.insert(0, str((Path(__file__).parent / "src").absolute()))
        env = os.environ.copy()
        env['PYTHONPATH'] = "./src"
        completed = subprocess.run(args, stdout=sys.stdout, stderr=sys.stderr, timeout=1200, env=env,
                                   cwd=str(tmp_path))
        assert completed.returncode == 0, f"FAILED TO EXECUTE pytest from \"{' '.join(args)}\" from " \
                                          f"{str(Path(__file__).parent.absolute())}"


def test_remote_execution_thread(tmp_path, chdir):
    chdir.chdir(Path(__file__).parent.parent)
    root = Path(__file__).parent.parent
    project_config = ProjectConfig(
        src_paths=[Path("src"), Path("testsrc"), Path("resources") / "requirements.txt"],
        test_files=[Path("test/*.py")],
        project_root=root,
        project_name="pytest_mproc_test"
    )
    args = list(sys.argv)
    sys.argv = ["test/test_mproc_runs.py", "-k", "alg2", "--cores", "4"]
    ipname = socket.gethostbyname(socket.gethostname())
    port = find_free_port()
    command = [
        shutil.which("python3"), "-m", "pytest", "--as-main", f"{ipname}:{port}",
    ]
    command.extend(sys.argv)
    env = os.environ.copy()
    import binascii
    env['AUTH_TOKEN_STDIN'] = '1'
    env['PYTHONPATH'] = "src"
    main_proc = subprocess.Popen(command, stdout=sys.stdout, stderr=sys.stderr, cwd=str(Path(__file__).parent.parent),
                                 env=env, stdin=subprocess.PIPE)
    main_proc.stdin.write(binascii.b2a_hex(current_process().authkey) + b'\n')
    main_proc.stdin.close()
    client_hosts = [
        RemoteHostConfig(ipname),
        RemoteHostConfig(ipname),
        RemoteHostConfig(ipname),
    ]
    thread = RemoteExecutionThread(project_config=project_config,
                                   remote_sys_executable=shutil.which("python3"),
                                   remote_hosts_config=client_hosts,
                                   )
    try:
        sem = Semaphore(0)
        time.sleep(3)
        mgr = OrchestrationManager((ipname, port))
        # noinspection PyUnresolvedReferences
        results_q = mgr.get_results_queue()
        thread.start_workers(
            server=ipname,
            server_port=port,
            auth_key=current_process().authkey,
            finish_sem=sem,
            result_q=results_q,
        )
        sem.release()
        sem.release()
        sem.release()
        thread.join(timeout=3*240)
    finally:
        sys.argv = args
        try:
            assert main_proc.wait(timeout=5) == 0
        except (subprocess.TimeoutExpired, TimeoutError):
            main_proc.terminate()
            raise Exception("Main process failed to terminate")
