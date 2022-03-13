import asyncio
import datetime
import json
import logging
import os
import platform
import sys
import tempfile
import zipfile
from asyncio import Semaphore
from multiprocessing.process import current_process

from typing.io import TextIO

from pytest_mproc.ptmproc_data import RemoteHostConfig
from pytest_mproc.remote.ssh import SSHClient, CommandExecutionFailure

from contextlib import suppress
try:
    from contextlib import asynccontextmanager
except ImportError:
    from aiocontext import async_contextmanager as asynccontextmanager

from pathlib import Path
from typing import Union, List, Tuple, Optional, Any, Dict, AsyncGenerator, Iterable

import pytest

_root = os.path.dirname(__file__)
FIVE_MINUTES = 5*60


class Bundle:

    def __init__(self, root_dir: Path, tests_dir: Path, requirements_paths: List[Path], system_executable: str,
                 env: Optional[Dict[str, str]] = None):
        """
        create a bundle for remote host deployment
        :param root_dir: a delicious carbonated beverage (and also the root where bundle files are created)
        """
        self._remote_executable = system_executable
        self._requirements_paths = requirements_paths
        if not root_dir.exists():
            raise FileNotFoundError(f"'{root_dir}' does not exist")
        if not tests_dir.exists():
            raise FileNotFoundError(f"'{tests_dir}' does not exist")
        self._sources: List[Path] = []
        self._shiv_path = root_dir / "test_exec.pyz"
        if os.path.exists(self._shiv_path):
            raise FileExistsError(f"File {self._shiv_path} already exists")
        self._test_zip_path = root_dir / "tests.zip"
        self._requirements_zip_path = root_dir / "requirements.zip"
        if os.path.exists(self._test_zip_path):
            raise FileExistsError(f"File {self._test_zip_path} already exists")
        # noinspection SpellCheckingInspection
        self._site_pkgs = tempfile.TemporaryDirectory()
        self._sources.append(Path(self._site_pkgs.name))
        self._resources: List[Tuple[Path, Path]] = []
        self._tests_dir = tests_dir
        self._system = platform.system()
        self._machine = platform.machine()
        self._env = env or {}

    @staticmethod
    def remote_tests_dir(remote_root: Path) -> Path:
        return remote_root / "tests"

    def __enter__(self) -> "Bundle":
        self._site_pkgs.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._site_pkgs.__exit__(exc_type, exc_val, exc_tb)

    @property
    def shiv_path(self) -> Path:
        return self._shiv_path

    @property
    def tests_zip_path(self):
        return self._test_zip_path

    @property
    def tests_dir(self):
        return self._tests_dir

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
        if not path.exists() or not path.is_file():
            raise FileExistsError(f"File {path} does not exist or is not a file")
        self._resources.append((path, relative_path))

    @classmethod
    def from_config(cls, project_config: Path):
        """
        create Bundle from provided config file

        :param project_config:  path to project config
        :return: Bundle based on provided config

        :raises: pytest.UsageError if config file is invalidly format or contains invalid content
        """
        with open(project_config, 'r', encoding='utf-8') as in_file:
            try:
                json_data = json.load(in_file)
                root_dir = json_data['root_dir']
                requirements_paths = json_data['requirements_paths']
                test_dir = json_data['test_dirs']
                src_dirs: List[str] = json_data['src_dirs']
                resource_paths: Dict[str, str] = json_data['resources']
                if type(root_dir) != str or not Path(root_dir).exists():
                    raise ValueError("'root_dir' in project config file must be a string path that exists")
                root_dir = Path(root_dir)
                resources: List[Tuple[Path, Path]] = []
                try:
                    for local_path, remote_path in resource_paths.items():
                        if type(local_path) != str or type(remote_path) != str:
                            raise pytest.UsageError(f"'resource_paths' in {project_config} must be a dictionary"
                                                    "with key string representing a valid path of"
                                                    "string values that represent local paths on the remote host")
                        resources.append((Path(local_path), Path(remote_path)))
                except Exception:
                    raise pytest.UsageError(f"Unable to parse 'resources_path' in project config file {project_config}")
                src_paths: List[Path] = []
                try:
                    for src_dir in src_dirs:
                        if not type(src_dir) == str or not Path(src_dir).exists():
                            raise pytest.UsageError(f"{src_dir} provided in 'src_dirs' element of {project_config}"
                                                    " does not exist")
                        src_paths.append(Path(src_dir))
                except TypeError:
                    raise pytest.UsageError(f"'src_dirs' in {project_config} must be a list of string paths")
                if type(test_dir) != str or not Path(test_dir).exists():
                    raise pytest.UsageError(f"'test_dir' must be a valid string path in {project_config}")
                tests_dir = Path(test_dir)
                if not isinstance(requirements_paths, Iterable) :
                    raise pytest.UsageError(f"'requirements_paths' must be a valid list of string paths"
                                            f" in {project_config}")
                requirements_paths = [Path(p) for p in requirements_paths]
                return cls.create(root_dir=root_dir, src_dirs=src_paths, resources=resources, tests_dir=tests_dir,
                                  requirements_paths=requirements_paths)
            except json.JSONDecodeError:
                raise pytest.UsageError(f"Project config file {project_config} is improperly formatted")
            except (ValueError, KeyError) as e:
                raise pytest.UsageError(f"Project config file {project_config} does not contain expected data {str(e)}")
            except Exception:
                raise ValueError(
                    f"Unable to process file {project_config} as json containing project configuration info")

    @classmethod
    def create(cls, root_dir: Path,
               src_dirs: List[Path],
               requirements_paths: List[Path],
               tests_dir: Path,
               resources: Optional[List[Tuple[Path, Path]]] = None,
               system_executable: Optional[str] = None,
               ) -> "Bundle":
        """
        Create a bundle
        :param root_dir: where artifacts are to be created
        :param src_dirs: location of sources (not test files) to be included
        :param tests_dir: path to where test Python files reside
        :param requirements_paths: list of paths to requirements.txt-formetted files
        :param resources: dir of any additional resources to be added to bundle (all files recursively underneath)
        :param system_executable: path ON REMOTE HOST of system python executable to use
        :return: created Bundle
        """
        import shiv.builder  # type: ignore
        import shiv.constants  # type: ignore
        from shiv import pip
        from shiv.bootstrap.environment import Environment  # type: ignore

        bundle = Bundle(root_dir=root_dir, tests_dir=tests_dir, requirements_paths=requirements_paths,
                        system_executable=system_executable)
        # noinspection SpellCheckingInspection
        for path, relpath in resources or []:
            bundle.add_file(path, relpath)
        bundle._sources.extend(src_dirs)
        for requirements_path in requirements_paths:
            pip.install(["--target", bundle._site_pkgs.name, "--force-reinstall", "-r", str(requirements_path)], )
        pip.install(["--target", bundle._site_pkgs.name, "--force-reinstall", "pytest"], )
        if not any([(Path(dir) / "pytest_mproc").exists for dir in src_dirs]):
            # mostly for running raw pytest_mproc code during unit/integration testing:
            pip.install(["--target", bundle._site_pkgs.name, "--force-reinstall", "pytest_mproc"], )
        else:
            root = Path(__file__).parent.parent.parent.parent
            setup = root / "setup.py"
            import subprocess
            assert subprocess.run([sys.executable, setup, "bdist_wheel"], cwd=str(root)).returncode == 0
            pip.install(["--target", bundle._site_pkgs.name, root/ "dist" / "pytest_mproc-4.2.0-py3-none-any.whl"])
        env = Environment(built_at=datetime.datetime.strftime(datetime.datetime.utcnow(),
                                                              shiv.constants.BUILD_AT_TIMESTAMP_FORMAT),
                          shiv_version="0.1.1")
        shiv.builder.create_archive(
            sources=bundle._sources,
            target=bundle._shiv_path,
            main="_bootstrap:bootstrap",
            env=env,
            compressed=True,
            interpreter=system_executable or f"/usr/bin/{Path(os.path.realpath(sys.executable)).name}"
        )
        return bundle

    @asynccontextmanager
    async def remote_root_context(self, ssh_client: SSHClient, remote_root: Optional[Path]) -> Path:
        if remote_root:
            yield remote_root
        else:
            async with ssh_client.mkdtemp() as root:
                yield root

    async def deploy(self, ssh_client: SSHClient, remote_root: Path, timeout: Optional[float] = None) -> None:
        """
        Deploy this bundle to a remote ssh client
        :param ssh_client: where to deploy
        :param timeout: raise TimeoutError if deployment takes too long
        :param remote_root: remote path to where bundle is deployed
        """
        # noinspection SpellCheckingInspection
        with zipfile.ZipFile(self.tests_zip_path, mode='w') as zfile:
            for f in self.tests_dir.absolute().glob("**/*"):
                if f.is_file() and '__pycache__' not in str(f) and not f.name.startswith('.'):
                    zfile.write(f, f"tests/{f.relative_to(self.tests_dir.absolute())}")
            for local, remote in self._resources:
                zfile.write(local, str(remote))
        with zipfile.ZipFile(self._requirements_zip_path, mode='w') as reqzfile:
            with tempfile.NamedTemporaryFile(mode='a+') as out:
                out.write(self._system)
                out.write(self._machine)
                out.flush()
                reqzfile.write(out.name, "ptmproc_platform")
            with tempfile.NamedTemporaryFile(mode='a+') as out:
                for path in self._requirements_paths:
                    with open(path, 'r') as in_stream:
                        out.write(in_stream.read())
                    out.write('\n')
                out.flush()
                reqzfile.write(out.name, 'requirements.txt')
        await ssh_client.mkdir(self.remote_tests_dir(remote_root))
        await ssh_client.push(self._shiv_path, remote_root / self._shiv_path.name)
        await ssh_client.push(self._test_zip_path, remote_root / self._test_zip_path.name)
        await ssh_client.push(self._requirements_zip_path, remote_root / self._requirements_zip_path.name)
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", ssh_client.destination,
            f"cd {str(remote_root)} && unzip {str(self._test_zip_path.name)}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        if proc.returncode != 0:
            text = await proc.stdout.read()
            raise CommandExecutionFailure(f"Failed to unzip tests on remote client {text}", proc.returncode)
        proc = await asyncio.subprocess.create_subprocess_exec(
            "ssh", ssh_client.destination,
            f"cd {str(remote_root)} && unzip {str(self._requirements_zip_path.name)}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        with suppress(Exception):
            await ssh_client.remove(self.remote_tests_dir(remote_root) / self._test_zip_path.name)
            await ssh_client.remove(remote_root / self._requirements_zip_path.name)
        if proc.returncode != 0:
            text = await proc.stdout.read()
            raise CommandExecutionFailure(f"Failed to unzip requirements on remote client {text}", proc.returncode)
        system, machine = await ssh_client.get_remote_platform_info()
        assert isinstance(system, str)
        if system != self._system or machine != self._machine:
            if "PYTHONPATH" in self._env:
                self._env["PYTHONPATH"] = str(remote_root / 'site-packages') + ':' + self._env["PYTHONPATH"]
            else:
                self._env["PYTHONPATH"] = str(remote_root / 'site-packages')
            await ssh_client.mkdir(remote_root / 'site_packages')
            await ssh_client.install(
                remote_py_executable=self._remote_executable,
                remote_root=remote_root,
                site_packages='site-packages',
                requirements_path='requirements.txt',
                cwd=remote_root
            )

    async def execute_remote_multi(
            self,
            remote_hosts_config: Union[List[RemoteHostConfig], AsyncGenerator[RemoteHostConfig, RemoteHostConfig]],
            finish_sem: Semaphore,
            *args,
            username: Optional[str] = None,
            password: Optional[str] = None,
            deploy_timeout: Optional[float] = FIVE_MINUTES,
            timeout: Optional[float] = None,
            uses_auth_key: bool = False,
            stdout: Optional[Union[TextIO, int]] = None,
            stderr: Optional[Union[TextIO, int]] = None,
    ) -> Dict[str, asyncio.subprocess.Process]:
        """
        deploy and execute to/on multiple host targets concurrently (async)

        :param remote_hosts_config: dictionary of hosts and arguments to pass to that specifi chost
        :param finish_sem: semaphore to signal termination eminent
        :param args: common args to pass to host when executing pytest
        :param username: optional username for login-based ssh
        :param password: optional password for login-based ssh
        :param deploy_timeout: optional timeout if deployment takes too long
        :param timeout: optional overall timeout for execution to complete on all hosts
        :return: Dict of host, proc where host is host process executed on and proc is the completed process
        """
        sem = Semaphore(0)
        tasks: List[asyncio.Task] = []
        if isinstance(remote_hosts_config, Iterable):
            for worker_config in remote_hosts_config:
                cli_args, cli_env = worker_config.argument_env_list()
                task = asyncio.get_event_loop().create_task(
                    self.execute_remote(worker_config.remote_host,
                                        *(list(args) + list(cli_args)),
                                        env=cli_env,
                                        username=username,
                                        password=password,
                                        deploy_timeout=deploy_timeout,
                                        timeout=timeout,
                                        remote_root=worker_config.remote_root,
                                        uses_auth_key=uses_auth_key,
                                        stdout=stdout,
                                        stderr=stderr),
                )
                tasks.append(task)
        else:
            async def lazy_distribution():
                async for worker_config in remote_hosts_config:
                    sem.release()
                    cli_args, cli_env = worker_config.argument_env_list()
                    task = asyncio.get_event_loop().create_task(
                        self.execute_remote(worker_config.remote_host,
                                            *(list(args) + list(cli_args)),
                                            uses_auth_key=uses_auth_key,
                                            env=cli_env,
                                            username=username,
                                            password=password,
                                            deploy_timeout=deploy_timeout,
                                            timeout=timeout,
                                            remote_root=worker_config.remote_root))
                    tasks.append(task)
                    if not finish_sem.locked():
                        break

            async def timeout():
                # if we cannot find a single client in time:
                await asyncio.wait_for(sem.acquire(), timeout=deploy_timeout)

            await asyncio.wait_for([lazy_distribution(), timeout()], timeout=None)
        results, _ = await asyncio.wait(tasks, timeout=timeout,
                                        return_when=asyncio.ALL_COMPLETED)
        results = [r.result() for r in results]
        procs = {host: proc for host, proc in results if not isinstance(proc, Exception)}
        exceptions = [e for e in results if isinstance(e, Exception)]
        if exceptions:
            for e in exceptions:
                logging.exception(e, msg=f"Error execution on remote host")
            if not procs:
                raise Exception(f"Exception(s) during remote execution on all remote hosts : {exceptions}")
        for proc in procs.values():
            if proc.returncode is not None:
                with suppress(OSError):
                    proc.kill()
        return procs

    async def execute_remote(self, host: str, *args,
                             remote_root: Path,
                             env: Optional[Dict[str, str]] = None,
                             username: Optional[str] = None,
                             password: Optional[str] = None,
                             deploy_timeout: Optional[float] = FIVE_MINUTES,
                             timeout: Optional[float] = None,
                             uses_auth_key: bool = False,
                             stdout: Optional[Union[TextIO, int]] = None,
                             stderr: Optional[Union[TextIO, int]] = None,
                             ) -> Tuple[str, asyncio.subprocess.Process]:
        """
        Execute tests on remote host
        :param host: which host
        :param args: args to pass on cmd line
        :param env: additional environment variables to set (key/value pairs)
        :param username: credentials if needed
        :param password: credentials if needed
        :param deploy_timeout: optional timeout on deploy
        :param timeout: optional timeout if execution takes too long; if None return proc instance immediately
            with no waiting
        :param remote_root: where on remote host to deploy bundle
        :return: asyncio.subprocess.Process instance
        """
        ssh_client = SSHClient(host, username, password)
        async with self.remote_root_context(ssh_client, remote_root) as remote_root:
            await self.deploy(ssh_client, remote_root=remote_root, timeout=deploy_timeout)
            command = f"{str(remote_root / self._shiv_path.name)} -m pytest"
            env.update(self._env)
            print(f">>>>>>>>>>>>>>>> LAUNCHING CLIENT: {command} {' '.join(args)}")
            proc = await ssh_client.execute_remote_cmd(command, *args,
                                                       timeout=timeout, env=env,
                                                       cwd=self.remote_tests_dir(remote_root),
                                                       uses_auth_key=uses_auth_key,
                                                       stdout=stdout, stderr=stderr)
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        return host, proc

    async def monitor_remote_execution(self, host: str, *args: str,
                                       username: Optional[str] = None,
                                       password: Optional[str] = None,
                                       deploy_timeout: Optional[float] = FIVE_MINUTES,
                                       timeout: Optional[float] = None,
                                       remote_root: Optional[Path] = None,
                                       ) -> AsyncGenerator[str, str]:
        args = ["-m", "pytest"] + list(args)
        ssh_client = SSHClient(host, username, password)
        async with self.remote_root_context(ssh_client, remote_root) as remote_root:
            await self.deploy(ssh_client, remote_root=remote_root, timeout=deploy_timeout)
            command = f"{str(remote_root / self._shiv_path.name)}"
            async for line in ssh_client.monitor_remote_execution(command, *args, timeout=timeout,
                                                                  cwd=self.remote_tests_dir(remote_root),
                                                                  ):
                yield line

