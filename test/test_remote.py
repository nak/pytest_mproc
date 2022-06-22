import glob
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

import pytest_mproc
from pytest_mproc import find_free_port
from pytest_mproc.ptmproc_data import ProjectConfig

from pytest_mproc.remote.bundle import Bundle
from pytest_mproc.remote.ssh import SSHClient, CommandExecutionFailure

_base_dir = Path(__file__).parent.parent
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__file__)
ssh_pswdless = (Path.home() / ".ssh" / "authorized_keys").exists()


class TestSSHClient:

    @pytest.mark.asyncio
    async def test_mkdtmp(self):
        ssh_client = SSHClient(host='localhost')
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
                test_files=[Path("test/*.py")],
                project_name="pytest_mproc_test"
            ),
            system_executable=sys.executable
    ) as bundle:
        yield bundle


def test_remote_execution_cli(tmp_path):
    root = Path(__file__).parent.parent
    project_config = ProjectConfig(
                                   test_files=[Path("test") / "*.py", Path("requirements.txt"),
                                               Path("src") / 'pytest_mproc' / '*.py',
                                               Path("src") / 'pytest_mproc' / 'remote' / '*.py',
                                               Path("testsrc/testcode/*.py"), Path("testsrc/testcode/**/*.py")],
                                   project_root=root,
                                   project_name="pytest_mproc_test"
                                   )
    project_config_path = tmp_path / "project.cfg"
    print("\n")
    for path in project_config.test_files:
        files = glob.glob(str(root / path))
        for f in files:
            os.makedirs( tmp_path / Path(f).relative_to(root).parent, exist_ok=True)
            shutil.copy(f, tmp_path / Path(f).relative_to(root))
    shutil.copy(root / "test" / "resources" / "requirements.txt", tmp_path / "requirements.txt")
    with open(project_config_path, 'w') as out:
        converted = {'test_files': [str(p) for p in project_config.test_files],
                     'project_name': project_config.project_name}
        out.write(json.dumps(converted))
        out.flush()
        # remote_host = 'fssh://pi@10.220.45.119:{find_free_port()}'
        remote_host = f'127.0.0.1:{find_free_port()}'
        pytest_mproc.Settings.set_ssh_credentials(username='pi')
        remote_server = f'delegated://pi@'
        client_connect = '10.220.45.110'
        client_connect = 'localhost'
        # remote_server = remote_host
        args = [
            'pytest', '-s', 'test_mproc_runs.py', '-k', 'alg2',
            '--cores', '3',
            '--as-main', f"{remote_server}",
            '--project-structure', str(project_config_path),
            '--remote-worker', client_connect,
            '--remote-worker', client_connect,
            '--remote-worker', client_connect,
            '--remote-worker', client_connect,
            '--remote-worker', client_connect,
        ]
        sys.path.insert(0, str((Path(__file__).parent / "src").absolute()))
        env = os.environ.copy()
        env['PYTHONPATH'] = f"{tmp_path}/src:{tmp_path}/test:{tmp_path}/testsrc"
        completed = subprocess.run(args, stdout=sys.stdout, stderr=sys.stderr, timeout=1200, env=env,
                                   cwd=str(tmp_path / 'test'))
        assert (tmp_path / "test" / "artifacts" / "artifacts-Worker-1.zip").exists()
        assert completed.returncode == 0, f"FAILED TO EXECUTE pytest from \"{' '.join(args)}\" from " \
                                          f"{str(Path(__file__).parent.absolute())}"
