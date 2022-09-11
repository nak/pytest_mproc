import asyncio
import os
import platform
from dataclasses import dataclass
from glob import glob

import sys
import tempfile
import zipfile
from contextlib import suppress
from pathlib import Path
from threading import RLock
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

from pytest_mproc.coordinator import Coordinator


from pytest_mproc import user_output
from pytest_mproc.ptmproc_data import ProjectConfig
from pytest_mproc.remote.ssh import SSHClient
from pytest_mproc.user_output import debug_print, always_print

_root = os.path.dirname(__file__)
FIFTEEN_MINUTES = 15*60
artifacts_root = os.environ.get('PTMPROC_HOST_ARTIFACTS_DIR', './artifacts')
lock = RLock()


@dataclass
class CoordinatorProcDescription:
    index: int
    ssh_client: SSHClient
    remote_artifacts_dir: Path
    cwd: Path
    env: Dict[str, str]
    timeout: Optional[float]


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
        self._relative_run_path = relative_run_dir or Path(".")
        for path in test_files:
            matched = glob(str(project_root / path), recursive=True)
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
        self._coordinator: Dict[str, Coordinator] = {}
        self._coordinator_descriptions: List[CoordinatorProcDescription] = []

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

    @property
    def zip_path(self):
        return self._zip_path

    @property
    def tests_files(self):
        return self._test_files

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
                        artifacts_path=project_config.artifacts_path,
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
        stdout = sys.stdout if user_output.is_verbose else asyncio.subprocess.DEVNULL
        file_exists = await ssh_client.remote_file_exists(Path(str(remote_venv) + '.success'))
        if not file_exists:
            with suppress(Exception):
                await ssh_client.rmdir(remote_venv)
            always_print(f"Installing python virtual environment to {remote_venv}")
            # noinspection PyBroadException
            try:
                await ssh_client.create_remote_venv(remote_py_executable=self._remote_executable,
                                                    remote_venv_path=remote_venv)
            except Exception:  # mostly keyboard error?
                with suppress(Exception):
                    await ssh_client.rmdir(remote_venv)
                raise
            else:
                await ssh_client.touch(remote_file_path=f"{str(remote_venv)}.success")

            await ssh_client.install_packages(
                'pytest', 'pytest_mproc',
                remote_executable=str(remote_venv / 'bin' / 'python3'),
                stdout=stdout,
                stderr=sys.stderr)
        else:
            always_print(f"Using cached remote virtual environment at {remote_venv} for execution")
        python_venv_exists = await ssh_client.remote_file_exists(remote_path=remote_venv / 'bin' / 'python3')
        if not python_venv_exists:
            with suppress(Exception):
                await ssh_client.rmdir(remote_venv)
            raise SystemError("!!! Creationg of python venv failed to create python3 executable in "
                              f"{remote_venv / 'bin' / 'python3'}")
        return remote_venv

    async def deploy(self, ssh_client: SSHClient, remote_root: Path, timeout: Optional[float] = None) -> None:
        """
        Deploy this bundle to a remote ssh client
        :param ssh_client: where to deploy
        :param timeout: raise TimeoutError if deployment takes too long
        :param remote_root: remote path to where bundle is deployed
        """
        always_print("Deploying bundle to remote worker %s...", ssh_client.host)
        try:
            await ssh_client.mkdir(self.remote_run_dir(remote_root), exists_ok=True)
            debug_print(f">>> Pushing contents to remote worker {ssh_client.host}...")
            await ssh_client.push(self._zip_path, remote_root / self._zip_path.name)
            always_print(f">>> Unpacking tests on remote worker {ssh_client.host}...")
            try:
                await ssh_client.unzip(remote_target_dir=remote_root,
                                       remote_zip_location=self._zip_path.name,
                                       timeout=timeout)
            finally:
                with suppress(Exception):
                    await ssh_client.remove(remote_root / self._zip_path.name)
            await ssh_client.mkdir(remote_root / "run" / self._relative_run_path, exists_ok=True)
        except Exception as e:
            always_print(f"!!! Failed to deploy to {ssh_client.host}: {e}")
            raise

    async def install_requirements(self, ssh_client: SSHClient, remote_root: Path, remote_venv: Path, ):
        if self._requirements_path:
            always_print(f"Installing requirements on remote worker {ssh_client.host} to {remote_venv}...")
            await ssh_client.install(
                venv=remote_venv,
                full_requirements_path=remote_root / 'run' / self._requirements_path,
                stdout=sys.stdout,
                stderr=sys.stderr
            )
