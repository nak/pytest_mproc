import logging
import os
import shutil
import socket
import sys
from base64 import b64encode
from multiprocessing.process import current_process

import pytest
import subprocess
from asyncio import Semaphore
from pathlib import Path

from pytest_mproc.main import RemoteExecutionThread
from pytest_mproc.ptmproc_data import RemoteHostConfig, ProjectConfig

try:
    from pytest_mproc.remote.bundle import Bundle
    from pytest_mproc.remote.ssh import SSHClient, CommandExecutionFailure

    have_shiv = True
except ImportError:
    # probably client and don't have shiv
    have_shiv = False

_base_dir = Path(__file__).parent.parent
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__file__)
ssh_pswdless = (Path.home() / ".ssh" / "authorized_keys").exists()


class TestSSHClient:

    @pytest.mark.asyncio
    async def test_mkdtmp(self):
        ssh_client = SSHClient('localhost')
        async with ssh_client.mkdtemp() as tmpdir:
            assert Path(tmpdir).exists()  # this is localhost after all
        assert not Path(tmpdir).exists()


@pytest.fixture(scope='session')
def bundle(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp("remote_testing")
    with Bundle.create(
            root_dir=Path(str(tmpdir)),
            src_dirs=[_base_dir / "src", _base_dir / "testsrc"],
            requirements_paths=[_base_dir / "requirements.txt"],
            tests_dir=_base_dir / "test",
    ) as bundle:
        bundle.add_file(_base_dir / "test" / "run_test.sh", "scripts/run_test.sh")
        yield bundle


@pytest.mark.skipif(not have_shiv, reason="shiv not present for testing")
def test_create_bundle(bundle):
    proc = subprocess.run([bundle.shiv_path, "-m", "pytest", ".", "-k", "alg1"], cwd=Path(__file__).parent,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT)
    assert proc.returncode == 0, proc.stdout


@pytest.mark.skipif(not ssh_pswdless, reason="No permission to ssh to localhost without password input")
@pytest.mark.skipif(not have_shiv, reason="shiv not present for testing")
@pytest.mark.asyncio
async def test_execute_bundle(bundle):
    lines = []
    try:
        async for line in bundle.monitor_remote_execution("localhost", ".", "-k", "alg1", timeout=100):
            lines.append(line)
            print(line)
    except CommandExecutionFailure:
        text = '\n'.join(lines)
        assert False, f"Execution failed:\n{text}"


@pytest.mark.asyncio
async def test_execute_remote_multi(bundle):
    hosts = [
        RemoteHostConfig("localhost", {"cores": "1"}),
        RemoteHostConfig("localhost", {"cores": "1"}),
        RemoteHostConfig("localhost", {"cores": "1"}),
    ]
    sem = Semaphore(0)
    procs = await bundle.execute_remote_multi(hosts, sem, ".", "-k", "alg3", deploy_timeout=100, timeout=200)
    for host, proc in procs.items():
        assert host == "localhost"
        assert proc.returncode != 0  # alg3 delibrately coded to fail


def test_remote_execution_thread(tmp_path):
    root = Path(__file__).parent.parent
    project_config = ProjectConfig(requirements_paths=[root / "test" / "resources" / "requirements1.txt"],
                                   src_paths=[root / "src", root / "testsrc"],
                                   tests_path=root / "test",
                                   )
    args = list(sys.argv)
    for arg in args:
        if arg.endswith(".py"):
            sys.argv.remove(arg)
        elif arg == "-k" or "execution_thread" in arg:
            sys.argv.remove(arg)
    sys.argv.extend(["test_mproc_runs.py"])
    command = [
        shutil.which("python3"), "-m", "pytest", "--as-server", f"{socket.gethostbyname(socket.gethostname())}:43210",
        "--cores", "0", "-k", "alg2",
        ' '.join(args)
    ]
    env = os.environ.copy()
    import binascii
    env['AUTH_TOKEN'] = binascii.b2a_hex(b'abcdef')
    current_process().authkey = b'abcdef'
    main_proc = subprocess.Popen(command, stdout=sys.stderr, stderr=sys.stderr, cwd=str(Path(__file__).parent),
                                 env=env)
    print(f">>>>>>>>>>>>> LAUNCHED {command} {main_proc.returncode}")
    client_hosts = [
        RemoteHostConfig("localhost"),
        RemoteHostConfig("localhost"),
        RemoteHostConfig("localhost"),
    ]
    thread = RemoteExecutionThread(project_config=project_config,
                                   remote_sys_executable=shutil.which("python3"),
                                   remote_hosts_config=client_hosts,
                                   )
    sem = Semaphore(0)
    thread.start(server=socket.gethostbyname(socket.gethostname()),
                 server_port=43210,
                 finish_sem=sem,
                 stdout=sys.stdout,
                 stderr=sys.stderr)
    sem.release()
    thread.join(timeout=240)
    if main_proc.returncode is None:
        main_proc.kill()
        assert False, "Main pytest process did not exit as expected"