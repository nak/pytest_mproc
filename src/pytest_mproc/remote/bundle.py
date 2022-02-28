import asyncio
import datetime
import os
import shlex
import shutil
import sys
import tempfile
import time

import paramiko
from pathlib import Path
from typing import Union, List, Tuple, Optional, Any, Callable

import shiv.builder  # type: ignore
import shiv.constants  # type: ignore
from shiv import pip
from shiv.bootstrap.environment import Environment  # type: ignore

_root = os.path.dirname(__file__)


class Bundle:

    def __init__(self, shiv_path: Union[Path, str]):
        self._sources: List[Path] = []
        if os.path.exists(shiv_path):
            raise FileExistsError(f"File {shiv_path} already exists")
        self._shiv_path = shiv_path if isinstance(shiv_path, Path) else Path(shiv_path)
        self._tmpdir: Optional[tempfile.TemporaryDirectory] = None  # type: ignore

    def __enter__(self) -> "Bundle":
        self._tmpdir = tempfile.TemporaryDirectory()
        self._site_pkgs = tempfile.TemporaryDirectory()
        self._site_pkgs.__enter__()
        self._sources.append(Path(self._tmpdir.name))
        self._sources.append(Path(self._site_pkgs.name))
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if exc_type is None:
                env = Environment(built_at=datetime.datetime.strftime(datetime.datetime.utcnow(),
                                                                      shiv.constants.BUILD_AT_TIMESTAMP_FORMAT),
                                  shiv_version="0.1.1")
                shiv.builder.create_archive(sources=self._sources,
                                            target=self._shiv_path,
                                            main="androidtestorchestrator.remote.client:main",
                                            env=env,
                                            compressed=True,
                                            interpreter=sys.executable)
        finally:
            self._site_pkgs.__exit__(exc_type, exc_val, exc_tb)
            if self._tmpdir is not None:
                self._tmpdir.__exit__(exc_type, exc_val, exc_tb)

    @property
    def shiv_path(self) -> Path:
        return self._shiv_path

    def add_file(self, path: Union[Path, str], relative_path: Union[Path, str]) -> None:
        """
        add given file to bundle
        :param path: path to the file to add
        :param relative_path: relative path within shiv zip app
        """
        if self._tmpdir is None:
            raise Exception("Did not enter context of bundle.")
        path = Path(path) if isinstance(path, str) else path
        relative_path = Path(relative_path) if isinstance(relative_path, str) else relative_path
        if relative_path.is_absolute():
            raise Exception("relative path to add must be relative, not absolute")
        full_path = Path(self._tmpdir.name).joinpath(relative_path)
        if full_path.exists():
            raise FileExistsError(f"File {relative_path} already exists within bundle")
        if not path.is_file():
            raise FileNotFoundError(f"File {path} does not exist or is not a file")
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        shutil.copy(path, full_path)

    @classmethod
    def create(cls, shiv_path: Path, src_dir: Path, requirements_path: Path,
               resources: Optional[List[Tuple[Union[str, Path], Union[str, Path]]]] = None) -> "Bundle":
        with Bundle(shiv_path) as bundle:
            for file in src_dir.glob(f"{str(src_dir)}/**/*.py"):
                bundle.add_file(file, file.relative_to(src_dir))
            for path, relpath in resources or []:
                bundle.add_file(path, relpath)
            requirements: List[str] = []
            with open(requirements_path, 'r') as file_in:
                for line in file_in:
                    requirements.append(line)
            pip.install(["--target", bundle._site_pkgs.name, *requirements])
        return bundle

    @staticmethod
    async def _execute_remote(ssh_client: paramiko.SSHClient, command, *args,
                              poll_interval: float = 0.5,  # seconds
                              timeout: Optional[float] = None):
        channel: paramiko.Channel = ssh_client._transport.open_session(timeout=timeout)
        channel.get_pty()
        start_time = time.monotonic()

        def getlines(data: str) -> Tuple[str, str]:
            """
            Split data into lines, but treat any last remnant as a buffer for client to combine with
            a next batch of data before decalring a full line
            :param data: string to process
            :return: a tuple of lines constained in a string and a buffer of data leftover at end
            """
            if '\n' in data:
                *lines, buffer = data.split()
                if data.endswith('\n'):
                    return buffer, ""
                else:
                    return lines, buffer
            else:
                return "", data
        channel.set_combine_stderr(True)
        with channel:
            full_command = command + ' ' + ' '.join(map(shlex.quote, args))
            channel.exec_command(full_command)
            buffer_stdout = ""
            while not channel.exit_status_ready():
                await asyncio.sleep(poll_interval)
                if channel.recv_ready():
                    data = buffer_stdout + channel.recv(10*1024)
                    lines, buffer_stdout = getlines(data)
                    for line in lines:
                        yield line
                if timeout and time.monotonic() - start_time > timeout:
                    channel.close()
                    raise TimeoutError("Failed to execute remote ssh command in time")
            while channel.recv_ready():
                data = buffer_stdout + channel.recv(10 * 1024)
                lines, buffer_stdout = getlines(data)
                for line in lines:
                    yield line
            if channel.recv_exit_status() != 0:
                raise SystemError(f"Command {command} return with exit status {channel.exit_status}")

    async def deploy(self, host: str, remote_root: Path, username: Optional[str] = None, password: Optional[str] = None,
                     polling_interval: float = 0.5):
        ssh_client = paramiko.SSHClient()
        ssh_client.connect(host, username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        try:
            sftp_client.chdir(remote_root)
        except IOError:
            sftp_client.mkdir(remote_root)
            sftp_client.chdir(remote_root)
        with sftp_client.open(remote_root / self.shiv_path.name, mode='wb') as out:
            with open(self._shiv_path, 'rb') as file_in:
                chunk_size = 100*1024
                data = file_in.read(chunk_size)
                while data:
                    out.write(data)
                    data = file_in.read(chunk_size)
                    await asyncio.sleep(polling_interval)

    async def execute_remote(self, host: str, *args, remote_root: Path = Path('.'),
                             username: Optional[str] = None, password: Optional[str] = None):
        ssh_client = paramiko.SSHClient()
        ssh_client.connect(host, username=username, password=password)
        await self.deploy(host=host, remote_root=remote_root, username=username, password=password)
        command = str(remote_root / self._shiv_path.name)
        async for line in await self._execute_remote(ssh_client, command, *args):
            yield line
