import asyncio
import json
import logging
import os
import platform

import pytest
import sys
import tempfile
import zipfile
from asyncio import Semaphore
from contextlib import suppress
from contextlib import asynccontextmanager
from pathlib import Path
from threading import RLock
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Iterable,
    List,
    Optional,
    TextIO,
    Tuple,
    Union,
)

from pytest_mproc import user_output
from pytest_mproc.ptmproc_data import RemoteHostConfig, ProjectConfig
from pytest_mproc.remote.ssh import SSHClient, CommandExecutionFailure
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc.remote import env

_root = os.path.dirname(__file__)
FIVE_MINUTES = 5*60
artifacts_root = os.environ.get('PTMPROC_HOST_ARTIFACTS_DIR', './artifacts')
lock = RLock()


class Bundle:

    # noinspection SpellCheckingInspection
    CACHE_DIR = Path(os.path.expanduser('~')).absolute() / ".ptmproc"

    def __init__(self, root_dir: Path, tests_dir: Path,
                 requirements_paths: List[Path],
                 pure_requirements_paths: List[Path],
                 system_executable: str,
                 prepare_script: Optional[Path] = None,
                 finalize_script: Optional[Path] = None,
                 env: Optional[Dict[str, str]] = None):
        """
        create a bundle for remote host deployment
        :param root_dir: a delicious carbonated beverage (and also the root where bundle files are created)
        """
        self._prepare_script = Path(prepare_script) if prepare_script else None
        self._finalize_script = Path(finalize_script) if finalize_script else None
        self._remote_executable = system_executable
        self._requirements_paths = requirements_paths
        self._pure_requirements_paths = pure_requirements_paths
        if not root_dir.exists():
            raise FileNotFoundError(f"'{root_dir}' does not exist")
        if not tests_dir.exists():
            raise FileNotFoundError(f"'{tests_dir}' does not exist")
        self._sources: List[Path] = []
        self._zip_path = root_dir / "bundle.zip"
        if os.path.exists(self._zip_path):
            raise FileExistsError(f"File {self._zip_path} already exists")
        # noinspection SpellCheckingInspection
        self._site_pkgs = tempfile.TemporaryDirectory()
        self._resources: List[Tuple[Path, Path]] = []
        self._tests_dir = tests_dir
        self._system = platform.system()
        self._machine = platform.machine()
        self._env = env or {}

    @staticmethod
    def remote_run_dir(remote_root: Path) -> Path:
        return remote_root / "run"

    @property
    def prepare_script(self):
        return self._prepare_script

    @property
    def finalize_script(self):
        return self._finalize_script

    def __enter__(self) -> "Bundle":
        self._site_pkgs.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._site_pkgs.__exit__(exc_type, exc_val, exc_tb)

    @property
    def zip_path(self):
        return self._zip_path

    @property
    def tests_dir(self):
        return self._tests_dir

    def add_file(self, path: Union[Path, str], relative_path: Union[Path, str]) -> None:
        """
        add given file to bundle
        :param path: path to the file to add
        :param relative_path: path relative remote root directory
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
                if 'pure_requirements_paths' in json_data:
                    pure_requirements_paths = json_data['requirements_paths']
                else:
                    pure_requirements_paths = []
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
                if not isinstance(requirements_paths, Iterable):
                    raise pytest.UsageError(f"'requirements_paths' must be a valid list of string paths"
                                            f" in {project_config}")
                requirements_paths = [Path(p) for p in requirements_paths]
                return cls.create(root_dir=root_dir,
                                  project_config=ProjectConfig(src_paths=src_paths,
                                                               resource_paths=resources,
                                                               tests_path=tests_dir,
                                                               requirements_paths=requirements_paths,
                                                               pure_requirements_paths=pure_requirements_paths
                                                               )
                                  )
            except json.JSONDecodeError:
                raise pytest.UsageError(f"Project config file {project_config} is improperly formatted")
            except (ValueError, KeyError) as e:
                raise pytest.UsageError(f"Project config file {project_config} does not contain expected data {str(e)}")
            except Exception:
                raise ValueError(
                    f"Unable to process file {project_config} as json containing project configuration info")

    @classmethod
    def create(cls, root_dir: Path,
               project_config: ProjectConfig,
               system_executable: Optional[str] = None,
               ) -> "Bundle":
        """
        Create a bundle
        :param root_dir: where artifacts are to be created
        :param project_config: project configuration defining contents of bundle
        :param system_executable: path ON REMOTE HOST of system python executable to use
        :return: created Bundle
        """
        bundle = Bundle(root_dir=root_dir,
                        tests_dir=project_config.tests_path,
                        requirements_paths=project_config.requirements_paths,
                        pure_requirements_paths=project_config.pure_requirements_paths,
                        prepare_script=project_config.prepare_script,
                        finalize_script=project_config.finalize_script,
                        system_executable=system_executable)
        # noinspection SpellCheckingInspection
        for path, relpath in project_config.resource_paths or []:
            bundle.add_file(path, relpath)
        bundle._sources.extend(project_config.src_paths)
        os.write(sys.stderr.fileno(), f"\n\n>>> Building bundle.  This may take a little while...\n\n".encode('utf-8'))
        site_pkgs = env.set_up_local(project_config=project_config, cache_dir=cls.CACHE_DIR)
        bundle._sources.append(site_pkgs)
        debug_print(">>> Zipping contents for remote worker...")
        with zipfile.ZipFile(bundle.zip_path, mode='w') as zfile:
            for f in bundle.tests_dir.absolute().glob("**/*"):
                if f.is_file() and '__pycache__' not in str(f) and not f.name.startswith('.'):
                    zfile.write(f, f"run/{f.relative_to(bundle.tests_dir.parent)}")
            if bundle.prepare_script:
                zfile.write(bundle.prepare_script, 'prepare')
            if bundle.finalize_script:
                zfile.write(bundle.finalize_script, 'finalize')
            for local, remote in bundle._resources:
                if remote.is_absolute():
                    raise pytest.UsageError(
                        f"Resrouce in project config {local}->{remote} does not specify a relative path as target")
                zfile.write(local, f"run/{str(remote)}")

            with tempfile.NamedTemporaryFile(mode='a+') as out:
                out.write(bundle._system)
                out.write(bundle._machine)
                out.flush()
                zfile.write(out.name, "host_ptmproc_platform")
            for source_path in bundle._sources:
                for f in source_path.absolute().glob("**/*"):
                    if f.is_file() and not f.name.startswith('.'):
                        zfile.write(f, f"site-packages/{f.relative_to(source_path)}")
            with tempfile.NamedTemporaryFile(mode='a+') as out:
                for path in bundle._requirements_paths:
                    with open(path, 'r') as in_stream:
                        out.write(in_stream.read())
                    out.write('\n')
                out.flush()
                zfile.write(out.name, 'requirements.txt')
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
        try:
            # noinspection SpellCheckingInspection
            await ssh_client.mkdir(self.remote_run_dir(remote_root))
            debug_print(f">>> Pushing contents to remote worker {ssh_client.host}...")
            await ssh_client.push(self._zip_path, remote_root / self._zip_path.name)
            debug_print(f">>> Unpacking tests on remote worker {ssh_client.host}...")
            # noinspection PyProtectedMember
            proc = await ssh_client._remote_execute(
                f"cd {str(remote_root)} && unzip {str(self._zip_path.name)}",
                stdout=sys.stdout if user_output.verbose else asyncio.subprocess.DEVNULL,
                stderr=sys.stderr
            )
            await asyncio.wait_for(proc.wait(), timeout=timeout)
            if proc.returncode != 0:
                text = await proc.stdout.read()
                raise CommandExecutionFailure(f"Failed to unzip tests on remote client {text}", proc.returncode)
            debug_print(f">>> Unzipping contents on remote worker {ssh_client.host}...")
            await asyncio.wait_for(proc.wait(), timeout=timeout)
            with suppress(Exception):
                await ssh_client.remove(remote_root / self._zip_path.name)
            if proc.returncode != 0:
                text = await proc.stdout.read()
                raise CommandExecutionFailure(f"Failed to unzip requirements on remote client {text}", proc.returncode)
            if self._requirements_paths:
                debug_print(f">>> Installing requirements on remote worker {ssh_client.host}...")
                assert not (remote_root / "site-packages" / "dataclasses.py").exists()
                await ssh_client.install(
                    remote_py_executable=self._remote_executable,
                    remote_root=remote_root,
                    site_packages='site-packages',
                    requirements_path='requirements.txt',
                    cwd=remote_root
                )
            debug_print(f">>> Done deploying to host {ssh_client.host}")
        except Exception as e:
            always_print(f"!!! Failed to deploy to {ssh_client.host}: {e}")
            raise

    async def execute_remote_multi(
            self,
            remote_hosts_config: Union[List[RemoteHostConfig], AsyncGenerator[RemoteHostConfig, RemoteHostConfig]],
            finish_sem: Semaphore,
            *args,
            tracker: Optional[Tuple[Dict[str, List[asyncio.subprocess.Process]], RLock]] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            deploy_timeout: Optional[float] = FIVE_MINUTES,
            timeout: Optional[float] = None,
            auth_key: Optional[bytes] = None,
            stdout: Optional[Union[TextIO, int]] = None,
            stderr: Optional[Union[TextIO, int]] = None,
    ) -> Dict[str, asyncio.subprocess.Process]:
        """
        deploy and execute to/on multiple host targets concurrently (async)

        :param tracker: dictionary to track threads associated with remote execution
        :param remote_hosts_config: dictionary of hosts and arguments to pass to that specifi chost
        :param finish_sem: semaphore to signal termination eminent
        :param args: common args to pass to host when executing pytest
        :param username: optional username for login-based ssh
        :param password: optional password for login-based ssh
        :param auth_key: auth token to use in multiprocessing authentication
        :param stdout: where to funnel stdout output
        :param stderr: where to funnel stderr output
        :param deploy_timeout: optional timeout if deployment takes too long
        :param timeout: optional overall timeout for execution to complete on all hosts
        :return: Dict of host, proc where host is host process executed on and proc is the completed process
        """
        sem = Semaphore(0)
        tasks: List[asyncio.Task] = []
        if isinstance(remote_hosts_config, Iterable):
            for index, worker_config in enumerate(remote_hosts_config):
                cli_args, cli_env = worker_config.argument_env_list()
                task = asyncio.get_event_loop().create_task(
                    self.execute_remote(worker_config.remote_host,
                                        *(list(args) + list(cli_args)),
                                        worker_id=f"Worker-{index}",
                                        env=cli_env,
                                        username=username,
                                        password=password,
                                        deploy_timeout=deploy_timeout,
                                        timeout=timeout,
                                        remote_root=worker_config.remote_root,
                                        auth_key=auth_key,
                                        stdout=stdout,
                                        stderr=stderr,
                                        tracker=tracker),
                )
                tasks.append(task)
        else:
            async def lazy_distribution():
                index = 0
                async for worker_config in remote_hosts_config:
                    sem.release()
                    cli_args, cli_env = worker_config.argument_env_list()
                    task = asyncio.get_event_loop().create_task(
                        self.execute_remote(worker_config.remote_host,
                                            *(list(args) + list(cli_args)),
                                            worker_id=f"Worker-{index}",
                                            auth_key=auth_key,
                                            env=cli_env,
                                            username=username,
                                            password=password,
                                            deploy_timeout=deploy_timeout,
                                            timeout=timeout,
                                            tracker=tracker,
                                            remote_root=worker_config.remote_root))
                    index += 1
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
        with open(Path("./artifacts") / "worker_info.txt", 'w') as out_stream:
            for index, r in enumerate(results):
                if not isinstance(r, Exception):
                    host, proc = r
                    out_stream.write(f"Worker-{index}: host={host}, pid={proc.pid}")
                else:
                    out_stream.write(f"Worker-{index}: <<see exception for this worker>>")
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
                             tracker: Tuple[Dict[str, List[asyncio.subprocess.Process]], RLock] = None,
                             stdout: Optional[Union[TextIO, int]] = None,
                             stderr: Optional[Union[TextIO, int]] = None,
                             auth_key: Optional[bytes] = None,
                             worker_id: Optional[str] = None,
                             ) -> Tuple[str, asyncio.subprocess.Process]:
        """
        Execute tests on remote host
        :param host: which host
        :param args: args to pass on cmd line
        :param env: additional environment variables to set (key/value pairs)
        :param tracker: dictionary to track threads associated with remote execution
        :param username: credentials if needed
        :param password: credentials if needed
        :param auth_key: token to use for multiprocessing authentication
        :param stdout: where to funnel stdout output
        :param stderr: where to funnel stderr output
        :param deploy_timeout: optional timeout on deploy
        :param timeout: optional timeout if execution takes too long; if None return proc instance immediately
            with no waiting
        :param remote_root: where on remote host to deploy bundle
        :param worker_id: Optional identifier to assign to worker
        :return: asyncio.subprocess.Process instance
        """
        ssh_client = SSHClient(host, username, password)
        async with self.remote_root_context(ssh_client, remote_root) as remote_root:
            await self.deploy(ssh_client, remote_root=remote_root, timeout=deploy_timeout)
            remote_artifacts_dir = remote_root / "artifacts"
            command = self._remote_executable
            env.update(self._env)
            if auth_key:
                env['AUTH_TOKEN_STDIN'] = "1"
            # noinspection SpellCheckingInspection
            env['PTMPROC_EXECUTABLE'] = command.split()[0]
            if "PYTHONPATH" in env:
                env["PYTHONPATH"] = str(remote_root / 'site-packages') + ':' + env["PYTHONPATH"]
            else:
                env["PYTHONPATH"] = str(remote_root / 'site-packages')
            if "PTMPROC_BASE_PORT" in os.environ:
                env["PTMPROC_BASE_PORT"] = os.environ["PTMPOC_BASE_PORT"]
            always_print(f">>> Executing tests on remote...")
            run_dir = self.remote_run_dir(remote_root)
            try:
                rel_path = Path(os.getcwd()).relative_to(self._tests_dir.parent)
                if len(rel_path.parts) > 1:
                    raise pytest.UsageError("You must run from the location of your project tests path "
                                            "(as defined in your project config file), or one level above that")
                if str(rel_path) != '.':
                    run_dir = run_dir / rel_path
            except ValueError:
                pass
            await ssh_client.mkdir(remote_root / "artifacts", exists_ok=True)
            pid = None
            try:
                if self.prepare_script:
                    proc = await ssh_client.execute_remote_cmd(
                        f"./prepare | tee \"{remote_root}/artifacts/{self._prepare_script.stem}.log\"",
                        prefix_cmd=f"set -o pipefail; ",
                        cwd=remote_root,
                        env=env,
                        timeout=timeout,
                        stdout=stdout if user_output.verbose else asyncio.subprocess.DEVNULL,
                        stderr=stderr if user_output.verbose else asyncio.subprocess.DEVNULL,
                    )
                    if proc.returncode != 0:
                        msg = f"!!! Failed to execute prepare script on host [{proc.returncode}]; " \
                              f"see log in artifacts. worker-{worker_id}/{self._prepare_script.stem}.log"
                        debug_print(msg)
                        raise SystemError(msg)
                cmd = f"{command} -m pytest {' '.join(args)} | tee {remote_root}/artifacts/pytest_stdout.log"
                env["PTMPROC_WORKER_ARTIFACTS_DIR"] = str(remote_artifacts_dir)
                proc = await ssh_client.execute_remote_cmd(
                    cmd,
                    timeout=timeout,
                    env=env,
                    cwd=run_dir,
                    auth_key=auth_key,
                    stdout=stdout,
                    stderr=stderr,
                    tracker=tracker,
                    stdin=None if auth_key is None else asyncio.subprocess.PIPE)
                pid = proc.pid
            finally:
                if self._finalize_script:
                    with suppress(Exception):
                        # noinspection SpellCheckingInspection
                        proc = await ssh_client.execute_remote_cmd(
                            f"./finalize  | tee \"{remote_root}/artifacts/{self._finalize_script.stem}.log\"",
                            cwd=remote_root,
                            prefix_cmd=f"set -o pipefail; ",
                            env=env,
                            timeout=timeout,
                            stdout=stdout if user_output.verbose else asyncio.subprocess.DEVNULL,
                            stderr=stderr if user_output.verbose else asyncio.subprocess.DEVNULL,
                        )
                        if proc.returncode:
                            debug_print(f"!!! WARNING: finalize script failed! ")
                try:
                    worker_id = worker_id or (f"{host}-{pid}" if pid else host)
                    destination_dir = (Path(artifacts_root) / worker_id).absolute()
                    destination_dir.mkdir(exist_ok=True, parents=True)
                    await ssh_client.execute_remote_cmd(cwd=remote_artifacts_dir,
                                                        command=f"zip -r {str(remote_root / 'artifacts.zip')} *",
                                                        stdout=asyncio.subprocess.DEVNULL,
                                                        stderr=asyncio.subprocess.DEVNULL)
                    await ssh_client.pull(remote_root / "artifacts.zip", destination_dir / "artifacts.zip",
                                          recursive=False)
                except CommandExecutionFailure as e:
                    os.write(sys.stderr.fileno(),
                             f"\n\n!!! Failed to pull {str(remote_artifacts_dir)} to {destination_dir}:\n"
                             f"    {e}\n\n".encode('utf-8'))
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
            command = self._remote_executable
            async for line in ssh_client.monitor_remote_execution(command, '-m', 'pytest', *args, timeout=timeout,
                                                                  cwd=self.remote_run_dir(remote_root) / "tests",
                                                                  ):
                yield line
