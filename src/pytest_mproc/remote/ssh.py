"""
SSH utilities for remote client interactions
"""
import asyncio
import logging
import shlex
import sys
import time
from contextlib import suppress
from multiprocessing.process import current_process
from pathlib import Path
from typing import Optional, Tuple, Dict, AsyncIterator, Callable, TextIO, Union

from aiocontext import async_contextmanager as asynccontextmanager
from dataclasses import dataclass

SUCCESS = 0


class CommandExecutionFailure(Exception):
    """
    Raised on failure to execute SSH command on remote client
    """

    def __init__(self, cmd: str, rc: int):
        super().__init__(f"Execution of '{cmd}' return error status {rc}")
        self._rc = rc

    @property
    def rc(self):
        return self._rc


@dataclass
class SSHClient:
    """
    SSH Client objct for interacting with a specific client
    """
    host: str
    username: Optional[str] = None
    password: Optional[str] = None

    _global_options = []

    @classmethod
    def set_global_options(cls, *args: str) -> None:
        """
        set ssh command options
        :param args: options to be applied to all invocations of ssh
        """
        cls._global_options = args

    @property
    def destination(self) -> str:
        """
        :return: host if no username provided, else "username@host" format for SSH destination
        """
        return f"{self.username}@{self.host}" if self.username else self.host

    async def mkdir(self, remote_path: Path, exists_ok=True):
        """
        make directory on remote client
        :param remote_path: path to create
        :param exists_ok: if True, do not raise error if directory already exists
        :raises: CommandExecutionFailure if command fails to execute (non-zero return code)
        """
        options = "-p" if exists_ok is True else ""
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", *self._global_options, self.destination,
            f"mkdir {options} {str(remote_path)}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        rc = await asyncio.wait_for(proc.wait(), timeout=5)
        if rc != SUCCESS:
            raise CommandExecutionFailure(f"mkdir -p {remote_path}", rc)

    async def rmdir(self, remote_path: Path):
        """
        Remove directory from remote client
        :param remote_path: which directory
        :raises: CommandExecutionFailure if command execution fails on remote client
        """
        proc = await asyncio.subprocess.create_subprocess_exec(
                "ssh", *self._global_options, self.destination,
                f"rm -r {remote_path}",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        rc = await asyncio.wait_for(proc.wait(), timeout=10)
        if rc != SUCCESS:
            raise CommandExecutionFailure(f"rm -rf {remote_path}", rc)

    async def remove(self, remote_path: Path):
        """
        Remove path from remote client

        :param remote_path: path to file to remove
        :raises: CommandExecutionFailure if command execution fails on remote client
        """
        proc = await asyncio.subprocess.create_subprocess_exec(
                "ssh", *self._global_options, self.destination,
                f"rm {remote_path}",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        rc = await asyncio.wait_for(proc.wait(), timeout=10)
        if rc != SUCCESS:
            raise CommandExecutionFailure(f"rm -rf {remote_path}", rc)

    async def push(self, local_path: Path, remote_path: Path, timeout: Optional[float] = None):
        """
        Puh file(s) to a remote client
        :param local_path: path to local file or (recursive) directory to copy
        :param remote_path: destination on remote client to copy to
        :param timeout: optional timeout for command execution
        :raises: TimeoutError if failed to execute in time
        :raises: CommandExecutionError if command fails to execute on remote client
        """
        if local_path.is_dir():
            args = ["scp", "-r", str(local_path), f"{self.destination}:{str(remote_path)}"]
        else:
            args = ["scp", str(local_path),  f"{self.destination}:{str(remote_path)}"]
        proc = await asyncio.subprocess.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        if timeout:
            rc = await asyncio.wait_for(proc.wait(), timeout=timeout)
        else:
            rc = await proc.wait()
        if rc != SUCCESS:
            raise CommandExecutionFailure(f"Copy from {str(local_path)} to {str(remote_path)}", rc)

    async def pull(self, remote_path: Path, local_path: Path, recursive: bool = True, timeout: Optional[float] = None):
        """
        Pull file(s) from remote client

        :param remote_path: file or directory to pull (recurseively) from remote client
        :param local_path: path to copy to local host
        :param recursive: specify True if copying a directory
        :param timeout: optional timeout for command to execute
        :raises: TimeoutError if command fails to execute in time
        :raises: CommendExecutionFailure if command faile to execute on remote host
        """
        if recursive:
            args = ["scp", "-r", f"{self.destination}:{str(remote_path)}",  str(local_path)]
        else:
            args = ["scp",  f"{self.destination}:{str(remote_path)}", str(local_path)]
        proc = await asyncio.subprocess.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        if timeout:
            rc = await asyncio.wait_for(proc.wait(), timeout=timeout)
        else:
            rc = await proc.wait()
        if rc != 0:
            raise CommandExecutionFailure(f"Copy from {str(local_path)} to {str(remote_path)}", rc)

    async def get_remote_platform_info(self):
        """
        :return:  Tuple of system and machine platform attributes of remote client
        """
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", *self._global_options, self.destination,
            f"uname -s && uname -p",
            stdout=asyncio.subprocess.PIPE,
            encoding='utf-8'
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
        if proc.returncode != 0:
            raise CommandExecutionFailure("Getting platform info through uname", proc.returncode)
        system, machine = stdout.strip().splitlines()
        if isinstance(system, bytes):
            system = system.decode('utf-8')
            machine = machine.decode('utf-8')
        return system, machine

    async def install(self, remote_py_executable: str,
                      remote_root: Path,
                      requirements_path: str,
                      site_packages: Optional[str] = None,
                      cwd: Optional[Path] = None):
        """
        install Python requirements  on remote client

        :param remote_py_executable: path to python executable to use on remote client
        :param remote_root: root directory on remote client
        :param requirements_path: path, relative to remote_root, where requirments file is to be found
        :param site_packages: if specific, a target site packages directory relative to remote_root where required
           packages are to be installed
        :raises: CommandExecutionFailure if install failes on remote client
        """
        remote_py_executable = remote_py_executable or sys.executable
        if site_packages:
            args = [
                "ssh", *self._global_options, self.destination,
                f"{remote_py_executable} -m pip install -t {str(remote_root / site_packages)} "
                f"-r {str( remote_root / requirements_path)}",
            ]
        else:
            args = [
                "ssh", self.destination,
                f"{remote_py_executable} -m pip install -r {str(remote_root / requirements_path)}",
            ]

        proc = await asyncio.subprocess.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(cwd) if cwd is not None else None
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            raise CommandExecutionFailure(f"{' '.join(args)} {stdout}",
                                          proc.returncode)

    async def monitor_remote_execution(self, command, *args,
                                       timeout: Optional[float] = None,
                                       cwd: Optional[Path] = None,
                                       stdin: Optional[Union[TextIO, int]] = None,
                                       success_code: Optional[Callable[[int], bool]] = lambda x: x == 0)\
            -> AsyncIterator[str]:
        """
        Monitor remote execution of a command asynchronously (asyncio)

        :param command: command to execute on remote client
        :param args: args to the command
        :param timeout: optional timeout for commnand
        :param cwd: directory on remote client in which to execute
        :param success_code: None if not to check return code, otherwise lambda taking return code an returning
           True is success or False otherwise (defaults to 0 being success code)
        :return: AsyncIterator of string providing line-by-line output from command
        :raises: TimeoutError if command fails to finish in time
        :raises: CommandExecutionFailure if command return non-success error code
        """
        start_time = time.monotonic()

        def getlines(data: str) -> Tuple[str, str]:
            """
            Split data into lines, but treat any last remnant as a buffer for client to combine with
            a next batch of data before declaring a full line

            :param data: string to process
            :return: a tuple of lines contained in a string and a buffer of data leftover at end
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
        proc = await self.launch_remote_command(command, *args,
                                                stdin=stdin,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.STDOUT, cwd=cwd)
        buffer_stdout = ""
        time_left = timeout or 0.0
        while time_left > 0.0:
            data = (await asyncio.wait_for(proc.stdout.read(10*1024), timeout=time_left)).decode('utf-8')
            lines, buffer_stdout = getlines(buffer_stdout + data)
            for line in lines:
                yield line
            time_left = timeout - time.monotonic() + start_time
            if not data:
                break
        if timeout is not None and time_left <= 0.0:
            with suppress(OSError):
                proc.terminate()
            with suppress(TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=5)
            if proc.returncode is None:
                with suppress(OSError):
                    proc.kill()
            raise TimeoutError(f"timeout waiting for ssh of command {command}")
        if buffer_stdout:
            yield buffer_stdout
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        if success_code and not success_code(proc.returncode):
            logging.error(f"process '{command} {' '.join(args)}' failed with code {proc.returncode}")
            raise CommandExecutionFailure(command, proc.returncode)

    @asynccontextmanager
    async def mkdtemp(self) -> Path:
        """
        :return: temporary directory created on remote client
        """
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", *self._global_options, self.destination,
            f"mktemp -d",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=5)
        if proc.returncode != 0:
            raise SystemError("Failed to create tmp dir on remote client")
        # noinspection SpellCheckingInspection
        tmpdir = (await proc.stdout.read()).decode('utf-8').strip()

        yield Path(tmpdir)

        await self.rmdir(Path(tmpdir))

    async def launch_remote_command(self,  command, *args,
                                    stdin: Union[TextIO, int],
                                    stdout: Union[TextIO, int],
                                    stderr: Union[TextIO, int],
                                    cwd: Optional[Path] = None,
                                    env: Optional[Dict[str, str]] = None) -> asyncio.subprocess.Process:
        """
        execute command on remote host

        :param command: command to execute
        :param args: args to command
        :param cwd: optional directory on remote host to execute under
        :param env: optional dictionary of environment variables for command
        :return: Process created (completed process if timeout is specified)
        :raises: TimeoutError if command does not execute in time (if timeout is specified)
        """
        full_command = command + ' ' + ' '.join(map(shlex.quote, args))
        if cwd is not None:
            full_command = f"cd {str(cwd)} && {full_command}"
        if env:
            for key, value in env.items():
                full_command = f"{key}={value} {full_command}"
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", *self._global_options, self.destination,
            f"{full_command}",
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            encoding="utf-8"
        )
        return proc

    async def execute_remote_cmd(self,  command, *args,
                                 uses_auth_key: bool,
                                 stdout: TextIO = sys.stdout,
                                 stderr: TextIO = sys.stderr,
                                 timeout: Optional[float] = None, cwd: Optional[Path] = None,
                                 env: Optional[Dict[str, str]] = None) -> asyncio.subprocess.Process:
        """
        execute command on remote host

        :param command: command to execute
        :param args: args to command
        :param timeout: optional timeout for command execution;  if None, function will return Process created
           without waiting for completion
        :param cwd: optional directory on remote host to execute under
        :param env: optional dictionary of environment variables for command
        :return: Process created (completed process if timeout is specified)
        :raises: TimeoutError if command does not execute in time (if timeout is specified)
        """
        proc = await self.launch_remote_command(command, *args, stdin=asyncio.subprocess.PIPE if uses_auth_key else None,
                                                stdout=stdout, stderr=stderr, cwd=cwd, env=env)
        if uses_auth_key:
            print(f">>>>>>>>>>>>>>>>>>> USING AUTH KEY {current_process().authkey} to {proc.stdin}")
            proc.stdin.write(current_process().authkey + b'\n')
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        return proc
