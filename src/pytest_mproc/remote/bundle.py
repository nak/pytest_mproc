import asyncio
import logging
import os
import platform
from glob import glob

import binascii
import sys
import tempfile
import zipfile
from contextlib import suppress
from pathlib import Path
from threading import RLock
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from pytest_mproc import user_output, get_auth_key, Settings, Constants
from pytest_mproc.fixtures import Node, Global
from pytest_mproc.ptmproc_data import RemoteWorkerConfig, ProjectConfig
from pytest_mproc.remote.ssh import SSHClient, CommandExecutionFailure, remote_root_context
from pytest_mproc.user_output import debug_print, always_print

_root = os.path.dirname(__file__)
FIFTEEN_MINUTES = 15*60
artifacts_root = os.environ.get('PTMPROC_HOST_ARTIFACTS_DIR', './artifacts')
lock = RLock()


class Bundle:

    CACHE_DIR = Path(os.path.expanduser('~')).absolute() / ".ptmproc"

    def __init__(self, root_dir: Path,
                 project_name: str,
                 project_root: Path,
                 artifacts_path: Path,
                 test_files: List[Path],
                 system_executable: str,
                 prepare_script: Optional[Path] = None,
                 finalize_script: Optional[Path] = None,
                 env: Optional[Dict[str, str]] = None,
                 relative_run_dir: Optional[Path] = None):
        """
        create a bundle for remote host deployment
        :param root_dir: a delicious carbonated beverage (and also the root where bundle files are created)
        :param test_files: location of test files, resources files, requirements files to upload to run tests
        :param system_executable: location of python executable on remote host, or None for default
        :param prepare_script: Optional path to script to run pre-test-execution
        :param finalize_script:: Optional path to run post-test-execution
        :param relative_run_dir: working directory, relative to root, where pytest should be executed; root dir if
           not specified
        """
        self._output_task = None
        self._project_name = project_name
        self._artifacts_path = artifacts_path
        if not root_dir.exists():
            raise FileNotFoundError(f"'{root_dir}' does not exist")
        if relative_run_dir is not None and relative_run_dir.is_absolute():
            raise ValueError("Relative run dir was provided as an absolute path when creating bundle")
        absolutes = [p for p in test_files if p.is_absolute()]
        if absolutes:
            raise ValueError(f"Files or wildcard patterns {absolutes} in project config file must be relative paths "
                             "to the project config location")
        self._zip_path = root_dir / "bundle.zip"
        if os.path.exists(self._zip_path):
            raise FileExistsError(f"File {self._zip_path} already exists")
        self._root_dir = Path(root_dir)
        self._prepare_script = Path(prepare_script) if prepare_script else None
        self._finalize_script = Path(finalize_script) if finalize_script else None
        self._remote_executable = system_executable or Path(sys.executable).name
        self._resources: List[Tuple[Path, Path]] = []
        self._requirements_path = None
        self._test_files = []
        self._site_pkgs = None
        self._relative_run_path = relative_run_dir or Path(".")
        for path in test_files:
            matched = glob(str(project_root / path))
            if not matched:
                raise ValueError(
                    f"files or wildcard pattern '{str(project_root.absolute() / path)}' did not match any files")
            self._test_files.extend([Path(p).relative_to(project_root) for p in matched])
        if any([not (project_root / p).exists() for p in self._test_files]):
            raise FileNotFoundError(f"One or more files in '{test_files}' do not exist in {project_root}")
        if Path('requirements.txt') in self._test_files:
            self._requirements_path = 'requirements.txt'
        self._system = platform.system()
        self._machine = platform.machine()
        self._env = env or {}

    @staticmethod
    def remote_run_dir(remote_root: Path) -> Path:
        """
        :return: Path on remote host where test code and resources are located for the run, given the root dir
           of installation of a bundle
        """
        return remote_root / "run"

    @property
    def prepare_script(self):
        return self._prepare_script

    @property
    def finalize_script(self):
        return self._finalize_script

    def __enter__(self) -> "Bundle":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    async def cleanup(self):
        if self._site_pkgs:
            client, path = self._site_pkgs
            ssh_client: SSHClient = client
            with suppress(Exception):
                await ssh_client.rmdir(path)

    @property
    def zip_path(self):
        return self._zip_path

    @property
    def tests_files(self):
        return self._test_files

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
    def create(cls,
               root_dir: Path,
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
        try:
            relative_run_dir = Path(os.getcwd()).relative_to(project_config.project_root.absolute())
        except ValueError:
            raise ValueError("You must execute pytest in the directory containing the project config, "
                             f"{str(project_config.project_root)} or a subdirectory thereof")
        bundle = Bundle(root_dir=root_dir,
                        project_name=project_config.project_name,
                        artifacts_path=project_config.artifcats_path,
                        project_root=project_config.project_root,
                        test_files=project_config.test_files,
                        prepare_script=project_config.prepare_script,
                        finalize_script=project_config.finalize_script,
                        relative_run_dir=relative_run_dir,
                        system_executable=system_executable)
        os.write(sys.stderr.fileno(), f"\n\n>>> Building bundle...\n\n".encode('utf-8'))
        always_print(">>> Zipping contents for remote worker...")
        with zipfile.ZipFile(bundle.zip_path, mode='w') as zip_file:
            for path in bundle._test_files:
                zip_file.write(project_config.project_root / path, f"run{os.sep}{str(path)}")
            if bundle.prepare_script:
                zip_file.write(project_config.project_root / bundle.prepare_script, 'prepare')
            if bundle.finalize_script:
                zip_file.write(project_config.project_root / bundle.finalize_script, 'finalize')
            with tempfile.NamedTemporaryFile(mode='a+') as out:
                out.write(bundle._system)
                out.write(bundle._machine)
                out.flush()
                zip_file.write(out.name, "host_ptmproc_platform")
        return bundle

    async def setup_remote_venv(self, ssh_client: SSHClient, remote_venv: Path) -> Path:
        """
        setup the remote base virtual environment (Python + pytest + pytest_mproc)
        """
        stdout = sys.stdout if user_output.verbose else asyncio.subprocess.DEVNULL
        proc = await ssh_client.execute_remote_cmd(
            'ls', '-d', str(remote_venv / 'bin' / 'python3'), stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )

        if proc.returncode != 0:
            always_print(f"Installing python virtual environment to {remote_venv}")
            proc = await ssh_client.execute_remote_cmd(
                self._remote_executable, '-m', 'venv', str(remote_venv),
                stdout=stdout,
                stderr=asyncio.subprocess.PIPE
            )
            if proc.returncode != 0:
                data = await proc.stderr.read()
                raise CommandExecutionFailure(
                    f"{self._remote_executable} -m venv {str(remote_venv)} failed:\n\n  {data}",
                    proc.returncode)
            await ssh_client.install_packages(
                'pytest', 'pytest_mproc',
                venv=remote_venv,
                cwd=remote_venv.parent,
                stdout=stdout,
                stderr=sys.stderr)

        return remote_venv

    async def deploy(self, ssh_client: SSHClient,
                     remote_venv: Path, remote_root: Path, timeout: Optional[float] = None) -> None:
        """
        Deploy this bundle to a remote ssh client
        :param remote_venv: location of Python virtual environment on remote host
        :param ssh_client: where to deploy
        :param timeout: raise TimeoutError if deployment takes too long
        :param remote_root: remote path to where bundle is deployed
        """
        always_print("Deploying bundle to remote worker %s...", ssh_client.host)
        stdout = sys.stdout if user_output.verbose else asyncio.subprocess.DEVNULL
        try:
            await ssh_client.mkdir(self.remote_run_dir(remote_root), exists_ok=True)
            debug_print(f">>> Pushing contents to remote worker {ssh_client.host}...")
            await ssh_client.push(self._zip_path, remote_root / self._zip_path.name)
            debug_print(f">>> Unpacking tests on remote worker {ssh_client.host}...")
            # noinspection PyProtectedMember
            proc = await ssh_client._remote_execute(
                f"cd {str(remote_root)} && unzip -o {str(self._zip_path.name)}",
                stdout=stdout,
                stderr=sys.stderr
            )
            await asyncio.wait_for(proc.wait(), timeout=timeout)
            if proc.returncode != 0:
                text = ('\n' + (await proc.stdout.read()).decode('utf-8')) if proc.stdout else ""
                raise CommandExecutionFailure(f"Failed to unzip tests on remote client {text}", proc.returncode)
            with suppress(Exception):
                await ssh_client.remove(remote_root / self._zip_path.name)
            if proc.returncode != 0:
                text = ('\n' + (await proc.stdout.read()).decode('utf-8')) if proc.stdout else ""
                raise CommandExecutionFailure(f"Failed to unzip requirements on remote client {text}", proc.returncode)
            if self._requirements_path:
                always_print(f"Installing requirements on remote worker {ssh_client.host} to {remote_venv}...")
                await ssh_client.install(
                    venv=remote_venv,
                    remote_root=remote_root,
                    requirements_path=f'run{os.sep}requirements.txt',
                    cwd=remote_root,
                    stdout=stdout,
                    stderr=sys.stderr
                )
            await ssh_client.mkdir(remote_root / "run" / self._relative_run_path, exists_ok=True)
        except Exception as e:
            always_print(f"!!! Failed to deploy to {ssh_client.host}: {e}")
            raise

    async def delegate(self, ssh_client: SSHClient,
                       delegation_port: int,
                       remote_root: Path, remote_venv: Path)\
            -> Tuple[asyncio.subprocess.Process, int, int]:
        """
        Launch a process to delegate main manager to a worker node
        This is done when the main node is blocked (by firewall) from receiving incoming data on a port

        :return: created process
        """
        root = Path(__file__).parent.parent.parent
        await ssh_client.mkdir(remote_root / 'site-packages' / 'pytest_mproc' / 'remote', exists_ok=True)
        await ssh_client.push(root / 'pytest_mproc' / '*.py', remote_root / 'site-packages' / 'pytest_mproc' / '.')
        await ssh_client.push(local_path=root / 'pytest_mproc' / 'remote' / '*.py',
                              remote_path=remote_root / 'site-packages' / 'pytest_mproc' / 'remote' / '.')
        self._site_pkgs = (ssh_client, remote_root / 'site-packages')
        await ssh_client.install_packages(
            'pytest', 'pytest_mproc',
            venv=remote_venv,
            cwd=remote_venv.parent,
            stdout=sys.stdout,
            stderr=sys.stderr)
        python = str(remote_venv / 'bin' / 'python3')
        python_path = os.environ.get('PYTHONPATH') or ''
        python_path = f"{str(remote_root / 'site-packages')}:{python_path}" if python_path \
            else f"{str(remote_root / 'site-packages')}"
        env = {'PYTHONPATH': python_path,
               'AUTH_TOKEN_STDIN': '1'}
        delegation_proc = await ssh_client.launch_remote_command(
            python, '-m', 'pytest_mproc.orchestration', ssh_client.host,
            str(delegation_port), self._project_name,
            env=env,
            stdout=sys.stdout,
            stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        delegation_proc.stdin.write(binascii.b2a_hex(get_auth_key()) + b'\n')
        delegation_proc.stdin.close()
        line = (await delegation_proc.stderr.readline()).decode('utf-8').strip()
        while not line.startswith("PORT:"):
            line = (await delegation_proc.stderr.readline()).decode('utf-8').strip()
        port_text = line.split(':', maxsplit=1)[-1]
        while not line.startswith("GLOBAL_PORT:"):
            line = (await delegation_proc.stderr.readline()).decode('utf-8').strip()
        global_port_text = line.split(':', maxsplit=1)[-1]

        async def read():
            line_ = (await delegation_proc.stderr.readline()).decode('utf-8')
            while line_:
                debug_print(line_.strip())
                line_ = (await delegation_proc.stdout.readline()).decode('utf-8')

        if user_output.verbose:
            self._output_task = asyncio.create_task(read())
        return delegation_proc, int(port_text), int(global_port_text)

    async def execute_remote_multi(
            self,
            server_info: Tuple[str, int],
            hosts_q: asyncio.Queue,
            port_q: asyncio.Queue,
            delegation_q: asyncio.Queue,
            deploy_timeout: Optional[float] = FIFTEEN_MINUTES,
            timeout: Optional[float] = None,
            auth_key: Optional[bytes] = None,
            env: Optional[Dict[str, str]] = None,
            delegation_port: Optional[int] = None,
    ) -> Tuple[Dict[str, asyncio.subprocess.Process], asyncio.subprocess.Process]:
        """
        deploy and execute to/on multiple host targets concurrently (async)

        :param delegation_q: if needed, queue to post delegated host
        :param delegation_port:
        :param port_q:
        :param hosts_q:
        :param server_info:
        :param auth_key: auth token to use in multiprocessing authentication
        :param deploy_timeout: optional timeout if deployment takes too long
        :param env: dict of environment variables to use on execution
        :param timeout: optional overall timeout for execution to complete on all hosts
        :return: Dict of host, proc where host is host process executed on and proc is the completed process
        """
        tasks: List[asyncio.Task] = []
        deployments: Dict[str, Tuple[Path, Path]] = {}
        contexts = {}
        deployment_tasks: Dict[str, Union[bool, asyncio.Task]] = {}
        delegation_proc: Optional[asyncio.subprocess.Process] = None
        try:
            env = env.copy() if env else {}
            index = 0
            worker_config: RemoteWorkerConfig = await hosts_q.get()
            server_host, server_port = server_info
            remote_roots: Dict[str, Path] = {}
            remote_venvs: Dict[str, Path] = {}
            while worker_config is not None:
                if '@' not in worker_config.remote_host:
                    username = Settings.ssh_username
                else:
                    username = None
                ssh_client = SSHClient(
                    host=worker_config.remote_host, username=username, password=Settings.ssh_password
                )
                if worker_config.remote_host not in contexts:
                    context = remote_root_context(self._project_name, ssh_client, worker_config.remote_root)
                    contexts[worker_config.remote_host] = context
                    (remote_roots[worker_config.remote_host], remote_venv_root) = await context.__aenter__()
                    remote_venvs[worker_config.remote_host] = remote_venv_root / 'venv'
                remote_root = remote_roots[worker_config.remote_host]
                remote_venv = remote_venvs[worker_config.remote_host]
                if worker_config.remote_host not in deployments:
                    await self.setup_remote_venv(ssh_client=ssh_client, remote_venv=remote_venv)
                if index == 0 and delegation_port is not None:
                    delegation_host = worker_config.remote_host
                    delegation_proc, server_port, global_server_port = await self.delegate(
                        ssh_client=ssh_client,
                        delegation_port=delegation_port, remote_venv=remote_venv,
                        remote_root=remote_root)
                    await port_q.put(server_port)
                    await port_q.put(global_server_port)
                    await delegation_q.put(delegation_host)
                    server_host = worker_config.remote_host
                index += 1
                if worker_config.remote_host not in deployment_tasks:
                    deployment_tasks[worker_config.remote_host] = asyncio.create_task(self.deploy(
                        ssh_client=ssh_client,
                        remote_root=remote_root,
                        remote_venv=remote_venv,
                        timeout=deploy_timeout
                    ))

                async def execute(worker_config_: RemoteWorkerConfig, worker_id: str) \
                        -> Tuple[str, asyncio.subprocess.Process]:
                    host_ = worker_config_.remote_host
                    # we must wait if deployment is not finished
                    # this setup parallel-izes multiple host efficiently
                    if deployment_tasks[host_]:
                        await deployment_tasks[host_]
                        deployment_tasks[host_] = False
                    if 'jump_host' in worker_config_.arguments:
                        jump_host = worker_config_.arguments['jump_host']
                        SSHClient.set_global_options('-J', jump_host)
                        del worker_config_.arguments['jump_host']
                    args, worker_env = _determine_cli_args(worker_config_, Constants.ptmproc_args, sys.argv[1:])
                    env.update(worker_env)
                    args += ["--as-worker", f"{server_host}:{server_port}"]
                    return await self.execute_remote(
                        *args,
                        worker_id=worker_id,
                        env=env,
                        cwd=remote_root / worker_id,
                        ssh_client=ssh_client,
                        timeout=timeout,
                        deploy_timeout=deploy_timeout,
                        remote_root=remote_root,
                        auth_key=auth_key,
                        remote_venv=remote_venv,
                        deploy=False
                    )
                task = asyncio.get_event_loop().create_task(execute(worker_config, worker_id=f"Worker-{index}"))
                tasks.append(task)
                if worker_config.remote_host not in deployments:
                    deployments[ssh_client.host] = (remote_root, remote_venv)
                worker_config = await hosts_q.get()
            results, _ = await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.ALL_COMPLETED)
            results = [r.result() for r in results]
            self._artifacts_path.mkdir(exist_ok=True)
            with open(self._artifacts_path / "worker_info.txt", 'w') as out_stream:
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
                    logging.exception(msg=f"Error execution on remote host", exc_info=e)
                if not procs:
                    raise Exception(f"Exception(s) during remote execution on all remote hosts : {exceptions}")
            for proc in procs.values():
                if proc.returncode is None:
                    with suppress(OSError):
                        proc.kill()
            return procs, delegation_proc
        finally:
            for context in contexts.values():
                await context.__aexit__(None, None, None)
            if self._output_task:
                with suppress(Exception):
                    await asyncio.wait_for(self._output_task, timeout=1)
                with suppress(Exception):
                    self._output_task.cancel()

    async def execute_remote(self, *args,
                             cwd: Path,
                             remote_root: Path,
                             remote_venv: Path,
                             env: Optional[Dict[str, str]] = None,
                             ssh_client: SSHClient,
                             timeout: Optional[float] = None,
                             deploy_timeout: Optional[float] = None,
                             auth_key: Optional[bytes] = None,
                             worker_id: Optional[str] = None,
                             deploy: bool = True
                             ) -> Tuple[str, asyncio.subprocess.Process]:
        """
        Execute tests on remote host
        :param deploy: whether to deploy or not (aka assume deployment already taken care of)
        :param deploy_timeout: timeout on deployments after this many seconds
        :param cwd: directory to run from
        :param ssh_client: ssh client to use to execute remotely
        :param args: args to pass on cmd line
        :param env: additional environment variables to set (key/value pairs)
        :param auth_key: token to use for multiprocessing authentication
        :param timeout: optional timeout if execution takes too long; if None return proc instance immediately
            with no waiting
        :param remote_root: where on remote host to deploy bundle
        :param remote_venv: location of virtual Python environment to use on host (possibly from cache)
        :param worker_id: Optional identifier to assign to worker
        :return: asyncio.subprocess.Process instance
        """
        if deploy:
            await self.deploy(
                ssh_client=ssh_client,
                remote_root=remote_root,
                remote_venv=remote_venv,
                timeout=deploy_timeout
            )
        command = str(remote_venv / 'bin' / 'python3')
        env.update(self._env)
        if auth_key:
            env['AUTH_TOKEN_STDIN'] = "1"
        # noinspection SpellCheckingInspection
        env['PTMPROC_EXECUTABLE'] = command.split()[0]
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = str(remote_root / 'site-packages') + ':' + env["PYTHONPATH"]
        else:
            env["PYTHONPATH"] = str(remote_root / 'site-packages')
        if user_output.verbose:
            env["PTMPROC_VERBOSE"] = '1'
        env['PTMPROC_NODE_MGR_PORT'] = str(Node.Manager.PORT)
        if "PTMPROC_BASE_PORT" in os.environ:
            env["PTMPROC_BASE_PORT"] = os.environ["PTMPROC_BASE_PORT"]
        always_print(f"Executing tests on remote worker %s via {command}...", ssh_client.host)
        run_dir = self.remote_run_dir(remote_root)
        await ssh_client.mkdir(cwd, exists_ok=True)
        await ssh_client.execute_remote_cmd("ln", "-sf", str(run_dir / '*'), str(cwd / '.'))
        remote_artifacts_dir = cwd / self._artifacts_path
        await ssh_client.mkdir(remote_artifacts_dir, exists_ok=True)
        cwd = cwd / self._relative_run_path
        pid = None
        proc = None
        stdout = sys.stdout if user_output.verbose else asyncio.subprocess.DEVNULL
        try:
            if self.prepare_script:
                always_print("Preparing environment through prepare script...")
                proc = await ssh_client.execute_remote_cmd(
                    f"./prepare | tee \"{remote_artifacts_dir}{os.sep}{self._prepare_script.stem}.log\"",
                    prefix_cmd=f"set -o pipefail; ",
                    cwd=cwd,
                    env=env,
                    timeout=timeout,
                    stdout=stdout,
                    stderr=sys.stderr,
                )
                if proc.returncode != 0:
                    msg = f"!!! Failed to execute prepare script on host [{proc.returncode}]; " \
                          f"see log in artifacts. worker-{worker_id}{os.sep}{self._prepare_script.stem}.log"
                    debug_print(msg)
                    raise SystemError(msg)
            cmd = f"{command} -m pytest {' '.join(args)} -s 2>&1 | "\
                  f"tee {remote_artifacts_dir}{os.sep}pytest_stdout.log"
            debug_print(">>> From %s running %s\n\n %s", cwd, cmd, env)
            await ssh_client.mkdir(remote_artifacts_dir, exists_ok=True)
            env["PTMPROC_WORKER_ARTIFACTS_DIR"] = str(remote_artifacts_dir)
            env["PTMPROC_VERBOSE"] = '1'
            env["PTMPROC_GM_URI"] = Global.Manager.singleton().uri
            proc = await ssh_client.execute_remote_cmd(
                cmd,
                timeout=timeout,
                env=env,
                prefix_cmd=f"set -o pipefail; ",
                cwd=cwd,
                auth_key=auth_key,
                stdout=sys.stdout,
                stderr=sys.stderr,
                stdin=None if auth_key is None else asyncio.subprocess.PIPE)
            pid = proc.pid
        finally:
            if proc and self._finalize_script:
                always_print("Finalizing worker through finalize script on client %s...", ssh_client.host)
                with suppress(Exception):
                    finalize_proc = await ssh_client.execute_remote_cmd(
                        f"./finalize  | tee \"{remote_artifacts_dir}{os.sep}]{self._finalize_script.stem}.log\"",
                        cwd=cwd,
                        prefix_cmd=f"set -o pipefail; ",
                        env=env,
                        timeout=timeout,
                        stdout=stdout,
                        stderr=sys.stderr,
                    )
                    if finalize_proc.returncode:
                        debug_print(f"!!! WARNING: finalize script failed! ")
            worker_id = worker_id or (f"{ssh_client.host}-{pid}" if pid else ssh_client.host)
            destination_dir = self._artifacts_path.absolute()
            try:
                if proc:
                    destination_dir.mkdir(exist_ok=True, parents=True)
                    await ssh_client.mkdir(remote_artifacts_dir, exists_ok=True)
                    await ssh_client.execute_remote_cmd(
                        cwd=remote_artifacts_dir,
                        command=f"zip -r {str(remote_artifacts_dir / 'artifacts.zip')} *",
                        stdout=stdout,
                        stderr=sys.stderr)
                    await ssh_client.pull(remote_artifacts_dir / "artifacts.zip",
                                          destination_dir / f"artifacts-{worker_id}.zip",
                                          recursive=False)
                    with zipfile.ZipFile(destination_dir / f"artifacts-{worker_id}.zip") as zip_file:
                        zip_file.extractall(destination_dir)
            except CommandExecutionFailure as e:
                always_print(f"Failed to pull artifacts from worker: {e}")
        return ssh_client.host, proc

    async def monitor_remote_execution(self, host: str, *args: str,
                                       username: Optional[str] = None,
                                       password: Optional[str] = None,
                                       deploy_timeout: Optional[float] = FIFTEEN_MINUTES,
                                       timeout: Optional[float] = None,
                                       remote_root: Optional[Path] = None,
                                       project_name: str,
                                       env: Optional[Dict[str, str]] = None,
                                       ) -> AsyncGenerator[str, str]:
        ssh_client = SSHClient(host=host, username=username, password=password)
        env = env or {}
        async with remote_root_context(project_name=project_name, ssh_client=ssh_client,
                                       remote_root=remote_root) as (remote_root, remote_venv_root):
            if "PYTHONPATH" in env:
                env["PYTHONPATH"] = str(remote_root / 'site-packages') + ':' + env["PYTHONPATH"]
            else:
                env["PYTHONPATH"] = str(remote_root / 'site-packages')
            await self.deploy(ssh_client, remote_root=remote_root, remote_venv=remote_venv_root / 'venv',
                              timeout=deploy_timeout)
            venv = remote_venv_root / 'venv'
            command = venv / 'bin' / 'python3'
            async for line in ssh_client.monitor_remote_execution(
                    command, '-m', 'pytest', *args, timeout=timeout, env=env,
                    cwd=self.remote_run_dir(remote_root) / self._relative_run_path,
            ):
                yield line


def _determine_cli_args(worker_config: RemoteWorkerConfig, ptmproc_args: Dict[str, Any], sys_args: List[str]):
    worker_args, worker_env = worker_config.argument_env_list()
    # remove pytest_mproc cli args to pass to client (aka additional non-pytest_mproc args)
    args = list(sys_args)  # make a copy
    for arg in sys_args:
        typ = ptmproc_args.get(arg)
        if not typ:
            continue
        if arg in worker_args:
            try:
                index = args.index(arg)
                if typ != bool:
                    args.remove(args[index + 1])
                args.remove(args[index])
            except ValueError:
                raise ValueError("No argument supplied for --cores option")
        elif typ in (bool,) and arg in args:
            args.remove(arg)
        elif arg in args and arg != '--cores':
            index = args.index(arg)
            if index + 1 < len(args):
                args.remove(args[index + 1])
            args.remove(arg)
    args += worker_args
    if "--cores" not in args:
        args += ["--cores", "1"]
    return args, worker_env
