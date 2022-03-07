import asyncio
import datetime
import os
import shlex
import shutil
import sys
import tempfile
import time
import zipfile
from contextlib import suppress

from pathlib import Path
from typing import Union, List, Tuple, Optional, Any

import shiv.builder  # type: ignore
import shiv.constants  # type: ignore
from dataclasses import dataclass
from shiv import pip
from shiv.bootstrap.environment import Environment  # type: ignore

_root = os.path.dirname(__file__)
FIVE_MINUTES = 5*60


@dataclass
class SSHClient:
    host: str
    username: Optional[str] = None
    password: Optional[str] = None

    @property
    def destination(self):
        return f"{self.username}@{self.host}" if self.username else self.host


class Bundle:

    def __init__(self, root_dir: Path, remote_root: Path = Path('.')):
        """
        create a bundle for remote host deployment
        :param root_dir: a delicious carbonated beverage (and also the roo where bundle files are created)
        :param remote_root: root location (path) on remote host where files while be pushed
        """
        self._sources: List[Path] = []
        self._remote_root = remote_root
        self._shiv_path = root_dir / "test_exec.pyz"
        if os.path.exists(self._shiv_path):
            raise FileExistsError(f"File {self._shiv_path} already exists")
        self._test_zip_path = root_dir / "tests.zip"
        if os.path.exists(self._test_zip_path):
            raise FileExistsError(f"File {self._test_zip_path} already exists")
        self._site_pkgs = tempfile.TemporaryDirectory()
        self._remote_tmpdirs: List[Tuple[SSHClient, str]] = []
        self._sources.append(Path(self._site_pkgs.name))

    @property
    def remote_tests_dir(self):
        return self._remote_root / "tests"

    def __enter__(self) -> "Bundle":
        self._site_pkgs.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._site_pkgs.__exit__(exc_type, exc_val, exc_tb)
        asyncio.get_event_loop().run_until_complete(self.cleanup())

    @property
    def shiv_path(self) -> Path:
        return self._shiv_path

    @property
    def tests_zip_path(self):
        return self._test_zip_path

    def add_file(self, path: Union[Path, str], relative_path: Union[Path, str]) -> None:
        """
        add given file to bundle
        :param path: path to the file to add
        :param relative_path: relative path within shiv zip app
        """
        path = Path(path) if isinstance(path, str) else path
        relative_path = Path(relative_path) if isinstance(relative_path, str) else relative_path
        if relative_path.is_absolute():
            raise Exception("relative path to add must be relative, not absolute")
        full_path = Path(self._site_pkgs.name).joinpath(relative_path)
        if full_path.exists():
            raise FileExistsError(f"File {relative_path} already exists within bundle")
        if not path.is_file():
            raise FileNotFoundError(f"File {path} does not exist or is not a file")
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        shutil.copy(path, full_path)

    @classmethod
    def create(cls, root_dir: Path,
               src_dirs: List[Path],
               requirements_path: Path,
               tests_dir: Path,
               resources: Optional[List[Tuple[Path]]] = None,
               remote_root: Path = Path('.')) -> "Bundle":
        """
        Create a bundle
        :param root_dir: where artifacts are to be created
        :param src_dirs: location of sources (not test files) to be included
        :param requirements_path: path to requirements.txt file
        :param tests_dir: location of test python files
        :param resources: dir of any additional resources to be added to bundle (all files recursively underneath)
        :return: create Bundle
        """
        bundle = Bundle(root_dir=root_dir, remote_root=remote_root)
        for path, relpath in resources or []:
            bundle.add_file(path, relpath)
        requirements: List[str] = []
        bundle._sources.extend(src_dirs)
        with zipfile.ZipFile(bundle.tests_zip_path, mode='w') as zfile:
            for f in tests_dir.absolute().glob("**/*"):
                if f.is_file() and '__pycache__' not in str(f) and not f.name.startswith('.'):
                    zfile.write(f, f"tests/{f.relative_to(tests_dir.absolute())}")
        with open(requirements_path, 'r') as file_in:
            for line in file_in:
                if line.strip() != "shiv":
                    requirements.append(line.strip())
        pip.install(["--target", bundle._site_pkgs.name, "--force-reinstall", *requirements],)
        env = Environment(built_at=datetime.datetime.strftime(datetime.datetime.utcnow(),
                                                              shiv.constants.BUILD_AT_TIMESTAMP_FORMAT),
                          shiv_version="0.1.1")
        shiv.builder.MAIN_TEMPLATE = "import _bootstrap\n_bootstrap.bootstrap()\n" + shiv.builder.MAIN_TEMPLATE
        shiv.builder.create_archive(sources=bundle._sources,
                                    target=bundle._shiv_path,
                                    main="pytest:main",
                                    env=env,
                                    compressed=True,
                                    interpreter=f"/usr/bin/{Path(os.path.realpath(sys.executable)).name}"
                                    )
        return bundle

    async def _execute_remote_cmd(self, ssh_client: SSHClient, command, *args, timeout: Optional[float] = None,
                                  cwd: Optional[Path] = None):
        start_time = time.monotonic()

        def getlines(data: str) -> Tuple[str, str]:
            """
            Split data into lines, but treat any last remnant as a buffer for client to combine with
            a next batch of data before decalring a full line
            :param data: string to process
            :return: a tuple of lines constained in a string and a buffer of data leftover at end
            """
            if '\n' in data:
                *lines, buffer = data.splitlines()
                if data.endswith('\n'):
                    lines.append(buffer)
                    return lines, ""
                else:
                    return lines, buffer
            else:
                return "", data
        full_command = command + ' ' + ' '.join(map(shlex.quote, args))
        if cwd is not None:
            full_command = f"cd {str(cwd)} && {full_command}"
        proc = await asyncio.subprocess.create_subprocess_exec("ssh", ssh_client.destination, f"{full_command}",
                                                               stdout=asyncio.subprocess.PIPE,
                                                               stderr=asyncio.subprocess.STDOUT,
                                                               )
        buffer_stdout = b""
        time_left = timeout or 0.0
        while time_left > 0.0:
            data = buffer_stdout + await asyncio.wait_for(proc.stdout.read(10*1024), timeout=time_left)
            lines, buffer_stdout = getlines(data.decode('utf-8'))
            for line in lines:
                yield line
            time_left = timeout - time.monotonic() + start_time
        if timeout is not None and time_left <= 0.0:
            raise TimeoutError(f"timeout waiting for ssh of command {full_command}")
        data = await proc.stdout.read(10 * 1024)
        with suppress(asyncio.TimeoutError):
            while data:
                lines, buffer_stdout = getlines(data.decode('utf-8'))
                for line in lines:
                    yield line
                data = buffer_stdout.encode('utf-8') + (await asyncio.wait_for(proc.stdout.read(10*1024), timeout=1))
            if buffer_stdout:
                yield buffer_stdout
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        if proc.returncode != 0:
            raise SystemError(f"Command {command} returned with exit status {proc.returncode} from remote host")

    async def cleanup(self):
        for ssh_client, path in self._remote_tmpdirs:
            proc = await asyncio.subprocess.create_subprocess_exec(
                "ssh", ssh_client.destination,
                f"rm -r {path}",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.wait(), timeout=5)

    async def mkdtemp(self, ssh_client: SSHClient):
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", ssh_client.destination,
            f"mktemp -d",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=5)
        if proc.returncode != 0:
            raise SystemError("Failed to create tmp dir on remote client")
        tmpdir = await proc.stdout.read()
        self._remote_tmpdirs.append((ssh_client, tmpdir.decode('utf-8')))
        return Path(tmpdir.decode('utf-8'))

    async def deploy(self, ssh_client: SSHClient, timeout: Optional[float] = None):
        proc = await asyncio.subprocess.create_subprocess_exec(
            "scp", str(self._shiv_path), f"{ssh_client.destination}:{str(self._remote_root / self._shiv_path.name)}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        if proc.returncode != 0:
            raise SystemError("Failed to transfer shiv file to remote client")

        proc = await asyncio.subprocess.create_subprocess_exec(
                "ssh", ssh_client.destination,
                f"mkdir -p {str(self.remote_tests_dir)}",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        await asyncio.wait_for(proc.wait(), timeout=5)
        if proc.returncode != 0:
            raise SystemError("Failed to mkdir on remote host")
        proc = await asyncio.subprocess.create_subprocess_exec(
            "scp",
            str(self._test_zip_path),
            f"{ssh_client.destination}:{str(self.remote_tests_dir / self._test_zip_path.name)}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        if proc.returncode != 0:
            raise SystemError("Failed to transfer tests zip file to remote client")
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", ssh_client.destination,
            f"cd {str(self.remote_tests_dir)} && unzip {str(self._test_zip_path.name)}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        if proc.returncode != 0:
            raise SystemError("Failed to unzip tests on remote client")

    async def execute_remote(self, host: str, *args,
                             username: Optional[str] = None,
                             password: Optional[str] = None,
                             deploy_timeout: Optional[float] = FIVE_MINUTES,
                             timeout: Optional[float] = None):
        ssh_client = SSHClient(host, username, password)
        await self.deploy(ssh_client, timeout=deploy_timeout)
        command = f"{str(self._remote_root / self._shiv_path.name)} -m pytest"
        async for line in self._execute_remote_cmd(ssh_client, command, *args, timeout=timeout,
                                                   cwd=self.remote_tests_dir):
            yield line
