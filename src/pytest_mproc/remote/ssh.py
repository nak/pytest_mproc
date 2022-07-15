"""
SSH utilities for remote client interactions
"""
import asyncio
import getpass
import logging
import os
import shlex
import subprocess
import sys
import time
from contextlib import suppress, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import (
    AsyncIterator,
    Callable,
    ContextManager,
    Dict,
    List,
    Optional,
    TextIO,
    Tuple,
    Union,
)

from pytest_mproc import user_output, Settings
from pytest_mproc.user_output import debug_print, always_print

SUCCESS = 0
TIMEOUT_SIMPLE_CMD = 15


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
    SSH Client class for interacting with a specific client
    """
    host: str
    username: str
    password: Optional[str] = None

    _global_options = []

    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.username = kwargs.get('username', getpass.getuser())
        self.password = kwargs.get('password')

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

    async def _remote_execute(self, command: str,
                              stdout, stderr, stdin=None,
                              env=None,
                              cwd=None,
                              prefix_cmd: Optional[str] = None,
                              auth_key: Optional[bytes] = None,
                              shell: bool = True) -> asyncio.subprocess.Process:
        """
        Execute given command on remote client a la ssh
        :param command: command to execute
        :return: proc
        """
        env = env or {}
        if auth_key is not None:
            env["AUTH_TOKEN_STDIN"] = '1'
        if env:
            for key, value in env.items():
                command = f"{key}=\"{value}\" {command}"
        if user_output.is_verbose:
            env["PTMPROC_VERBOSE"] = '1'
        if cwd is not None:
            command = f"cd {str(cwd)} && {prefix_cmd or ''} {command}"
        command = command.replace('"', '\\\"')
        full_cmd = f"ssh {self.destination} {' '.join(self._global_options)} \"{command}\""
        debug_print(f"Executing command '%s'", full_cmd)
        if shell:
            result = await asyncio.subprocess.create_subprocess_shell(
                full_cmd,
                stdout=stdout,
                stderr=stderr,
                stdin=stdin,
            )
            return result
        else:
            return await asyncio.subprocess.create_subprocess_exec(
                'ssh', self.destination, *self._global_options, f"\"command\"",
                stdout=stdout,
                stderr=stderr,
                stdin=stdin,
            )

    async def mkdir(self, remote_path: Path, exists_ok=True):
        """
        make directory on remote client
        :param remote_path: path to create
        :param exists_ok: if True, do not raise error if directory already exists
        :raises: CommandExecutionFailure if command fails to execute (non-zero return code)
        """
        options = "-p" if exists_ok is True else ""
        proc = await self._remote_execute(
            f"mkdir {options} {str(remote_path)}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        rc = await asyncio.wait_for(proc.wait(), timeout=TIMEOUT_SIMPLE_CMD)
        if rc != SUCCESS:
            stdout = await proc.stdout.read()
            if not exists_ok or b'File exists' not in stdout:
                raise CommandExecutionFailure(f"mkdir -p {remote_path}:\n\n{stdout.decode('utf-8')}", rc)

    async def rmdir(self, remote_path: Path):
        """
        Remove directory from remote client
        :param remote_path: which directory
        :raises: CommandExecutionFailure if command execution fails on remote client
        """
        proc = await self._remote_execute(
            f"rm -r {remote_path}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        rc = await asyncio.wait_for(proc.wait(), timeout=TIMEOUT_SIMPLE_CMD)
        if rc != SUCCESS:
            raise CommandExecutionFailure(f"rm -rf {remote_path}", rc)

    async def remove(self, remote_path: Path):
        """
        Remove path from remote client

        :param remote_path: path to file to remove
        :raises: CommandExecutionFailure if command execution fails on remote client
        """
        proc = await self._remote_execute(
            f"rm {remote_path}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        rc = await asyncio.wait_for(proc.wait(), timeout=TIMEOUT_SIMPLE_CMD)
        if rc != SUCCESS:
            raise CommandExecutionFailure(f"rm -rf {remote_path}", rc)

    async def push(self, local_path: Union[Path, List[Path]], remote_path: Path, timeout: Optional[float] = None):
        """
        Puh file(s) to a remote client
        :param local_path: path to local file or (recursive) directory to copy
        :param remote_path: destination on remote client to copy to
        :param timeout: optional timeout for command execution
        :raises: TimeoutError if failed to execute in time
        :raises: CommandExecutionError if command fails to execute on remote client
        """
        if isinstance(local_path, list):
            file_list = ' '.join([f"\"{str(p)}\"" for p in local_path])
            args = list(self._global_options) +\
                ['-r', '-p', file_list, f"{self.destination}:{str(remote_path)}"]
        elif local_path.is_dir():
            args = list(self._global_options) +\
                ['-r', '-p', str(local_path), f"{self.destination}:{str(remote_path)}"]
        else:
            args = list(self._global_options) +\
                ['-p', str(local_path), f"{self.destination}:{str(remote_path)}"]
        debug_print(f"Executing command 'scp {' '.join(args)}")
        proc = await asyncio.create_subprocess_shell(
            f"scp {' '.join(args)}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        if timeout:
            rc = await asyncio.wait_for(proc.wait(), timeout=timeout)
        else:
            rc = await proc.wait()
        if rc != SUCCESS:
            stdout = (await proc.stdout.read()).decode('utf-8')
            stderr = (await proc.stderr.read()).decode('utf-8')
            raise CommandExecutionFailure(f"Copy from {str(local_path)} to {str(remote_path)} "
                                          f"[scp {' '.join(args)}] \n\n{stdout}\n\n {stderr}\n\n",
                                          rc=rc)

    async def pull(self, remote_path: Path, local_path: Path,
                   recursive: bool = True, timeout: Optional[float] = None) -> None:
        """
        Pull file(s) from remote client

        :param remote_path: file or directory to pull (recursively) from remote client
        :param local_path: path to copy to local host
        :param recursive: specify True if copying a directory
        :param timeout: optional timeout for command to execute
        :raises: TimeoutError if command fails to execute in time
        :raises: CommendExecutionFailure if command failed to execute on remote host
        """
        if recursive or local_path.is_dir():
            args = ['-p', '-r',  f"{self.destination}:{str(remote_path)}", str(local_path.absolute())]
        else:
            args = ['-p', f"{self.destination}:{str(remote_path)}", str(local_path.absolute())]
        args = list(self._global_options) + args
        cmd = f"scp {' '.join(args)}"
        debug_print(f"Executing '{cmd}'")
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        if timeout:
            rc = await asyncio.wait_for(proc.wait(), timeout=timeout)
        else:
            rc = await proc.wait()
        if rc != 0:
            stdout = (await proc.stdout.read()).decode('utf-8')
            msg = f"\n!!! Copy failed from  {self.destination}:{str(remote_path)} to "\
                  f"{str(local_path.absolute())} [{cmd}]\n"\
                  f"\n{stdout}\n\n"
            always_print(msg)
            raise CommandExecutionFailure(msg, rc)

    def pull_sync(self, remote_path: Path, local_path: Path,
                  recursive: bool = True, timeout: Optional[float] = None) -> None:
        """
        Pull file(s) from remote client

        :param remote_path: file or directory to pull (recursively) from remote client
        :param local_path: path to copy to local host
        :param recursive: specify True if copying a directory
        :param timeout: optional timeout for command to execute
        :raises: TimeoutError if command fails to execute in time
        :raises: CommendExecutionFailure if command failed to execute on remote host
        """
        if recursive or local_path.is_dir():
            args = ['-p', '-r',  f"{self.destination}:{str(remote_path)}", str(local_path.absolute())]
        else:
            args = ['-p', f"{self.destination}:{str(remote_path)}", str(local_path.absolute())]
        args = list(self._global_options) + args
        cmd = f"scp {' '.join(args)}"
        debug_print(f"Executing '{cmd}'")
        proc = subprocess.run(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            shell=True,
            timeout=timeout
        )
        if proc.returncode != 0:
            stdout = proc.stdout.decode('utf-8')
            msg = f"\n!!! Copy from  {self.destination}:{str(remote_path)} to {str(local_path.absolute())} [{cmd}]\n"\
                  f"\n{stdout}\n\n"
            always_print(msg)
            raise CommandExecutionFailure(msg, proc.returncode)

    async def get_remote_platform_info(self):
        """
        :return:  Tuple of system and machine platform attributes of remote client
        """
        proc = await self._remote_execute(
            f"uname -s && uname -p",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=TIMEOUT_SIMPLE_CMD)
        if proc.returncode != 0:
            raise CommandExecutionFailure("Getting platform info through uname", proc.returncode)
        system, machine = stdout.strip().splitlines()
        if isinstance(system, bytes):
            system = system.decode('utf-8')
            machine = machine.decode('utf-8')
        return system, machine

    async def install_packages(
            self,  *pkgs,
            remote_executable: str,
            stdout: Optional[Union[int, TextIO]] = None,
            stderr: Optional[Union[int, TextIO]] = sys.stderr):
        cmd = f"{remote_executable} -m pip install --upgrade {' '.join(pkgs)}"
        proc = await self._remote_execute(
            cmd,
            stdout=stdout,
            stderr=stderr,
        )
        stdout, _ = await proc.communicate()
        assert proc.returncode is not None
        if proc.returncode != 0:
            always_print(f"Command {cmd} failed to install on remote", as_error=True)
            raise CommandExecutionFailure(cmd, proc.returncode)

    async def install(self,
                      venv: Path,
                      full_requirements_path: Path,
                      stdout: Optional[Union[int, TextIO]] = None,
                      stderr: Optional[Union[int, TextIO]] = sys.stderr):
        """
        install Python requirements  on remote client

        :param venv: path to python virtual environment on remote host
        :param full_requirements_path: path, relative to remote_root, where requirements file is to be found
        :param stdout: as per subprocess.Popen
        :param stderr: as per subprocess.Popen
        :raises: CommandExecutionFailure if install failed on remote client
        """
        remote_py_executable = venv / 'bin' / 'python3'
        cmd = f"{str(remote_py_executable)} -m pip install --upgrade -r {str(full_requirements_path)}"
        proc = await self._remote_execute(
            cmd,
            stdout=stdout,
            stderr=stderr,
        )
        stdout, _ = await proc.communicate()
        assert proc.returncode is not None
        if proc.returncode != 0:
            always_print(f"Command {cmd} failed to install on remote", as_error=True)
            raise CommandExecutionFailure(cmd, proc.returncode)

    async def monitor_remote_execution(self, command, *args,
                                       timeout: Optional[float] = None,
                                       cwd: Optional[Path] = None,
                                       stdin: Optional[Union[TextIO, int]] = None,
                                       env: Optional[Dict[str, str]] = None,
                                       success_code: Optional[Callable[[int], bool]] = lambda x: x == 0)\
            -> AsyncIterator[str]:
        """
        Monitor remote execution of a command asynchronously (asyncio)

        :param command: command to execute on remote client
        :param args: args to the command
        :param timeout: optional timeout for command
        :param cwd: directory on remote client in which to execute
        :param success_code: None if not to check return code, otherwise lambda taking return code and returning
           True is success or False otherwise (defaults to 0 being success code)
        :param stdin: stream to use for stdin (as in subprocess.Popen)
        :param env: optional dict of environment vars to apply

        :return: AsyncIterator of string providing line-by-line output from command
        :raises: TimeoutError if command fails to finish in time
        :raises: CommandExecutionFailure if command return non-success error code
        """
        start_time = time.monotonic()

        def getlines(data_: str) -> Tuple[str, str]:
            """
            Split data into lines, but treat any last remnant as a buffer for client to combine with
            a next batch of data before declaring a full line

            :param data_: string to process
            :return: a tuple of lines contained in a string and a buffer of data leftover at end
            """
            if '\n' in data_:
                *lines_, buffer = data_.splitlines()
                if data_.endswith('\n'):
                    lines_.append(buffer)
                    return lines_, ""
                else:
                    return lines_, buffer
            else:
                return "", data_
        proc = await self.launch_remote_command(command, *args,
                                                stdin=stdin,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.STDOUT, cwd=cwd,
                                                env=env or os.environ)
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
            with suppress(TimeoutError, KeyboardInterrupt):
                await asyncio.wait_for(proc.wait(), timeout=TIMEOUT_SIMPLE_CMD)
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

    @contextmanager
    def mkdtemp(self, tmp_root: Optional[Path] = None) -> Path:
        """
        :return: temporary directory created on remote client
        """
        if tmp_root is None:
            # noinspection SpellCheckingInspection
            proc = self._remote_execute_sync(
                f"mktemp -d --tmpdir ptmproc-tmp.XXXXXXXXXX",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                shell=True
            )
        else:
            # noinspection SpellCheckingInspection
            proc = self._remote_execute_sync(
                f"mktemp -d  {str(tmp_root / 'ptmproc-tmp.XXXXXXXXXX')}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                shell=True
            )
        stdout, stderr = proc.communicate(timeout=TIMEOUT_SIMPLE_CMD)
        if proc.returncode != 0:
            raise SystemError(f"Failed to create tmp dir on remote client: {stdout}\n  {stderr}")
        # noinspection SpellCheckingInspection
        tmpdir = stdout.decode('utf-8').strip()
        yield Path(tmpdir)
        self.rmdir_sync(Path(tmpdir))

    async def launch_remote_command(self,  command, *args,
                                    stdin: Union[TextIO, int],
                                    stdout: Union[TextIO, int],
                                    stderr: Union[TextIO, int],
                                    cwd: Optional[Path] = None,
                                    env: Optional[Dict[str, str]] = None,
                                    prefix_cmd: Optional[str] = None,
                                    auth_key: Optional[bytes] = None,
                                    shell: bool = True) -> asyncio.subprocess.Process:
        """
        execute command on remote host

        :param command: command to execute
        :param prefix_cmd: an optional prefix to insert before command in shell command string
        :param args: args to command
        :param cwd: optional directory on remote host to execute under
        :param stdin: stream to use for stdin (as in subprocess.Popen)
        :param stdout: stream to use for stdout (as in subprocess.Popen)
        :param stderr: stream to use for stderr (as in subprocess.Popen)
        :param env: optional dictionary of environment variables for command
        :param auth_key: authentication key for multiprocessing
        :param shell: whether to run in shell or not
        :return: Process created (completed process if timeout is specified)
        :raises: TimeoutError if command does not execute in time (if timeout is specified)
        """
        args = [shlex.quote(str(arg)) if '*' not in arg else arg for arg in args]
        full_command = str(command) + ' ' + ' '.join(args)
        result = await self._remote_execute(
            full_command,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            env=env,
            cwd=cwd,
            prefix_cmd=prefix_cmd,
            auth_key=auth_key,
            shell=shell
        )
        assert result
        return result

    async def execute_remote_cmd(self,  command, *args,
                                 stdout: Union[int, TextIO] = sys.stdout,
                                 stderr: Union[int, TextIO] = sys.stderr,
                                 stdin: Optional[Union[TextIO, int]] = None,
                                 timeout: Optional[float] = None,
                                 cwd: Optional[Path] = None,
                                 prefix_cmd: Optional[str] = None,
                                 env: Optional[Dict[str, str]] = None,
                                 auth_key: Optional[bytes] = None,
                                 shell: bool = True,
                                 ) -> asyncio.subprocess.Process:
        """
        execute command on remote host

        :param command: command to execute
        :param prefix_cmd: an optional prefix to insert before command in shell command string
        :param args: args to command
        :param timeout: optional timeout for command execution;  if None, function will return Process created
           without waiting for completion
        :param cwd: optional directory on remote host to execute under
        :param stdin: stream to use for stdin (as in subprocess.Popen)
        :param stdout: stream to use for stdout (as in subprocess.Popen)
        :param stderr: stream to use for stderr (as in subprocess.Popen)
        :param env: optional dictionary of environment variables for command
        :param auth_key: authentication key for multiprocessing
        :param shell: whether to run in shell or not
        :return: Process created (completed process if timeout is specified)
        :raises: TimeoutError if command does not execute in time (if timeout is specified)
        """
        proc = await self.launch_remote_command(command, *args,
                                                stdin=asyncio.subprocess.PIPE if auth_key is not None else stdin,
                                                stdout=stdout,
                                                stderr=stderr,
                                                cwd=cwd,
                                                auth_key=auth_key,
                                                prefix_cmd=prefix_cmd,
                                                shell=shell,
                                                env=env)
        if auth_key is not None:
            import binascii
            auth_key = binascii.b2a_hex(auth_key)
            proc.stdin.write(auth_key + b'\n')
            proc.stdin.close()
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        return proc

    def signal_sync(self, pgids: List[int], sig_number: int) -> None:
        """
        Kill processes with assigned pids on remote host.

        :raises CommandExecutionFailure: if it fails to kill, with exception if process is no longer active;  in that
           case this function returns normally
        """
        pid_texts = [str(-pgid) for pgid in pgids]
        try:
            command = f"kill -{sig_number} {' '.join(pid_texts)}"
            completed = subprocess.run(
                ["ssh", self.destination, *self._global_options, command],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=TIMEOUT_SIMPLE_CMD
            )
            if completed.returncode != 0 and b'No such process' not in completed.stdout:
                always_print(f"Failed to kill one or more worker pids {' '.join(pid_texts)} "
                             f"on {self.destination}: {completed.stdout.decode('utf-8')}", as_error=True)
        except TimeoutError:
            always_print("Timeout trying to kill remote worker processes")

    async def port_forward(self, local_port: int, remote_port: int):
        """
        Port forward from local machine to remote port
        :param local_port: port on local machine to forward connection requests
        :param remote_port: port on remote machine to accept (forwarded) connections requests
        """
        debug_print(f"Executing command 'ssh -N -L {local_port}:127.0.0.1:{remote_port} {self.destination}'")
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", "-N", "-L", f"{local_port}:127.0.0.1:{remote_port}", f"{self.destination}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        await asyncio.sleep(0.5)
        if proc.returncode is not None:
            stdout = (await proc.stdout.read()).decode('utf-8')
            raise CommandExecutionFailure(f"Failed to port forward: {stdout}", rc=proc.returncode)
        return proc

    async def reverse_port_forward(self, local_port: int, remote_port: int):
        """
        Reverse port forward from remote port to local machine
        :param local_port: port on local machine to accept (reverse forwarded) connection requests
        :param remote_port: port on remote machine to  forward connections requests
        """
        debug_print(f"Executing command 'ssh -N -R {remote_port}:127.0.0.1:{local_port} {self.destination}'")
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", "-N", "-R", f"{remote_port}:127.0.0.1:{local_port}", f"{self.destination}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        await asyncio.sleep(0.5)
        if proc.returncode is not None:
            stdout = (await proc.stdout.read()).decode('utf-8')
            raise CommandExecutionFailure(f"Failed to port forward: {stdout}", rc=proc.returncode)
        return proc

    async def find_free_port(self) -> int:
        """
        find free port on remote host
        """
        # noinspection SpellCheckingInspection
        proc = await self._remote_execute(
            "python3 -c \"import socket; s = socket.socket(); s.bind(('', 0));print(s.getsockname()[1]);s.close()\"",
            stdout=asyncio.subprocess.PIPE,
            stderr=sys.stderr,
            shell=True
        )
        text = (await proc.stdout.read()).decode('utf-8')
        return int(text.strip())

    def _remote_execute_sync(
        self, command: str,
        stdout, stderr, stdin=None,
        env=None,
        cwd=None,
        prefix_cmd: Optional[str] = None,
        auth_key: Optional[bytes] = None,
        shell: bool = True
    ) -> subprocess.Popen:
        """
        See equivalent async method docs
        """
        env = env or {}
        if auth_key is not None:
            env["AUTH_TOKEN_STDIN"] = '1'
        if env:
            for key, value in env.items():
                command = f"{key}=\"{value}\" {command}"
        if user_output.is_verbose:
            env["PTMPROC_VERBOSE"] = '1'
        if cwd is not None:
            command = f"cd {str(cwd)} && {prefix_cmd or ''} {command}"
        command = command.replace('"', '\\\"')
        full_cmd = f"ssh {self.destination} {' '.join(self._global_options)} \"{command}\""
        debug_print(f"Executing command '%s'", full_cmd)
        if shell:
            result = subprocess.Popen(
                full_cmd,
                stdout=stdout,
                stderr=stderr,
                stdin=stdin,
                shell=True,
            )
            assert result
            return result
        else:
            return subprocess.Popen(
                ['ssh', self.destination, *self._global_options, f"\"command\""],
                stdout=stdout,
                stderr=stderr,
                stdin=stdin,
            )

    def mkdir_sync(self, remote_path: Path, exists_ok=True):
        """
        See mkdir method docs
        """
        options = "-p" if exists_ok is True else ""
        proc = self._remote_execute_sync(
            f"mkdir {options} {str(remote_path)}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        rc = proc.wait(timeout=TIMEOUT_SIMPLE_CMD)
        if rc != SUCCESS:
            stdout = proc.stdout.read()
            if not exists_ok or b'File exists' not in stdout:
                raise CommandExecutionFailure(f"mkdir -p {remote_path}:\n\n{stdout.decode('utf-8')}", rc)

    def rmdir_sync(self, remote_path: Path):
        """
        See equivalent async method docs
        """
        proc = self._remote_execute_sync(
            f"rm -r {remote_path}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        rc = proc.wait(timeout=TIMEOUT_SIMPLE_CMD)
        if rc != SUCCESS:
            raise CommandExecutionFailure(f"rm -rf {remote_path}", rc)

    def launch_remote_command_sync(
        self,
        command,
        *args,
        stdin: Union[TextIO, int],
        stdout: Union[TextIO, int],
        stderr: Union[TextIO, int],
        cwd: Optional[Path] = None,
        env: Optional[Dict[str, str]] = None,
        prefix_cmd: Optional[str] = None,
        auth_key: Optional[bytes] = None,
        shell: bool = True
    ) -> subprocess.Popen:
        """
        execute command on remote host

        See equivalent async method docs
        """
        args = [shlex.quote(str(arg)) if '*' not in arg else arg for arg in args]
        full_command = str(command) + ' ' + ' '.join(args)
        return self._remote_execute_sync(
            full_command,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            env=env,
            cwd=cwd,
            prefix_cmd=prefix_cmd,
            auth_key=auth_key,
            shell=shell
        )

    def execute_remote_cmd_sync(
            self,  command, *args,
            stdout: Union[int, TextIO] = sys.stdout,
            stderr: Union[int, TextIO] = sys.stderr,
            stdin: Optional[Union[TextIO, int]] = None,
            timeout: Optional[float] = None,
            cwd: Optional[Path] = None,
            prefix_cmd: Optional[str] = None,
            env: Optional[Dict[str, str]] = None,
            auth_key: Optional[bytes] = None,
            shell: bool = True,
    ) -> subprocess.Popen:
        """
        execute command on remote host

        See equivalent async method docs
        """
        proc = self.launch_remote_command_sync(
            command, *args,
            stdin=asyncio.subprocess.PIPE if auth_key is not None else stdin,
            stdout=stdout,
            stderr=stderr,
            cwd=cwd,
            auth_key=auth_key,
            prefix_cmd=prefix_cmd,
            shell=shell,
            env=env)
        if auth_key is not None:
            import binascii
            auth_key = binascii.b2a_hex(auth_key)
            proc.stdin.write(auth_key + b'\n')
            proc.stdin.close()
        proc.wait(timeout=timeout)
        return proc

    async def ping(self, timeout: float) -> bool:
        try:
            await self.execute_remote_cmd(
                "echo", "PONG",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                timeout=timeout,
                shell=True,
            )
            return True
        except (TimeoutError, asyncio.TimeoutError):
            return False

    async def unzip(self, remote_zip_location: Union[Path, str], remote_target_dir: Path,
                    timeout: Optional[float] = None):
        proc = await self._remote_execute(
            f"cd {str(remote_target_dir)} && unzip -o {str(remote_zip_location)}",
            stdout=sys.stdout if user_output.is_verbose else asyncio.subprocess.DEVNULL,
            stderr=sys.stderr
        )
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        if proc.returncode != 0:
            text = ('\n' + (await proc.stdout.read()).decode('utf-8')) if proc.stdout else ""
            raise CommandExecutionFailure(f"Failed to unzip tests on remote client {text}", proc.returncode)

    async def remote_file_exists(self, remote_path: Path):
        proc = await self.execute_remote_cmd(
            'ls', str(remote_path),
            stderr=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
        )
        return proc.returncode == 0

    async def create_remote_venv(self, remote_py_executable: Union[Path, str], remote_venv_path: Path):
        proc = await self.execute_remote_cmd(
            str(remote_py_executable), '-m', 'venv', str(remote_venv_path),
            stdout=sys.stdout if user_output.is_verbose else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        if proc.returncode != 0:
            text = ('\n' + (await proc.stderr.read()).decode('utf-8')) if proc.stderr else ""
            raise CommandExecutionFailure(f"Failed to unzip tests on remote client {text}", proc.returncode)

    async def touch(self, remote_file_path: Union[Path, str]):
        proc = await self.execute_remote_cmd(
            f'touch {str(remote_file_path)}',
            stdout=sys.stdout if user_output.is_verbose else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            shell=True
        )
        if proc.returncode != 0:
            text = ('\n' + (await proc.stderr.read()).decode('utf-8')) if proc.stderr else ""
            raise CommandExecutionFailure(f"Failed to unzip tests on remote client {text}", proc.returncode)


@contextmanager
def remote_root_context(project_name: str, ssh_client: SSHClient, remote_root: Optional[Path]) -> \
        ContextManager[Tuple[Path, Path]]:
    """
    Provide a context representing a directory on the remote host from which to run.  If no explicit
    path is provided as a remote root, a temporary directory is created on remote host and removed
    when exiting this context.  (Otherwise context manager has no real effect)

    :param project_name: name of project, used in creating a cache dir if available to remote system
    :param ssh_client: ssh client to use in creating a temp dir if necessary
    :param remote_root: an explicit location on the remote host (will not be removed on exit of context)
    :return: the remote root created (or reflects the remote root provided explicitly)
    """
    if Settings.cache_dir is None:
        proc = ssh_client.execute_remote_cmd_sync('pwd', stdout=asyncio.subprocess.PIPE, shell=True)
        stdout_text = proc.stdout.read().strip().decode('utf-8')
        if (proc.returncode != 0 or not stdout_text) and remote_root is not None:
            cache_path = remote_root
        else:
            cache_path = Path(stdout_text) / '.ptmproc' / 'cache'
    else:
        cache_path = Settings.cache_dir
    venv_root = cache_path / project_name
    ssh_client.mkdir_sync(venv_root, exists_ok=True)
    debug_print(f"Using {cache_path / project_name} to cache python venv")
    with ssh_client.mkdtemp(remote_root or Settings.tmp_root) as root:
        always_print(f"Using dir {venv_root} to set up python venv and {root} as temp staging dir")
        yield root, venv_root or root
