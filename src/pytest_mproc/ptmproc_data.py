import os
from contextlib import suppress

import json
import pytest

import pytest_mproc

from aiofile import async_open
from pathlib import Path
from typing import Optional, Dict, List, Iterable, Tuple, AsyncGenerator, Any
from dataclasses import dataclass, field

from pytest_mproc.http import HTTPSession
from pytest_mproc.user_output import always_print

REQUIREMENTS_PATHS = 'requirements_paths'
TEST_FILES = 'test_files'


@dataclass
class ProjectConfig:
    project_name: str
    project_root: Path
    test_files: List[Path]
    artifcats_path: Optional[Path] = Path('./artifacts')
    prepare_script: Optional[Path] = None
    finalize_script: Optional[Path] = None

    @classmethod
    def from_file(cls, path: Path) -> "ProjectConfig":
        with open(path, 'rb') as in_stream:
            try:
                data = json.load(in_stream)
                if 'artifacts_path' in data:
                    if Path(data['artifacts_path']).is_absolute():
                        raise pytest.UsageError(
                            f"'artifacts_dir' in project config '{str(path)}' must be a relative path")
                if 'project_name' not in data:
                    raise pytest.UsageError(f"project config {str(path)} does not define 'project_name'")
                if not data['project_name'].strip():
                    raise pytest.UsageError(f"project_name in {str(path)} cannot be empty string")
                if 'finalize_script' in data and (not Path(data['finalize_script']).exists or
                                                  not os.access(data['finalize_script'], os.X_OK)):
                    raise pytest.UsageError(
                        f"finalize script {data['finalize_script']} either does not exist or is not executable")
                if 'prepare_script' in data and (not Path(data['prepare_script']).exists or
                                                 not os.access(data['prepare_script'], os.X_OK)):
                    raise pytest.UsageError(
                        f"prepare script {data['prepare_script']} either does not exist or is not executable")
                if TEST_FILES not in data:
                    raise pytest.UsageError(f"'{TEST_FILES}' is not in project config '{path}'")
                if type(data[TEST_FILES]) != list:
                    raise pytest.UsageError(f"{TEST_FILES} in project config '{path}' is not a list of paths")
                if any([type(f) != str for f in data[TEST_FILES]]):
                    raise pytest.UsageError(f"'test_path' must be a single string path in '{path}'")
                return ProjectConfig(
                    project_name=data['project_name'],
                    project_root=path.parent,
                    test_files=[Path(p) for p in data[TEST_FILES]],
                    prepare_script=data.get('prepare_script'),
                    artifcats_path=data.get('artifacts_path', Path("./artifacts")),
                    finalize_script=data.get('finalize_script'),
                )
            except json.JSONDecodeError:
                raise pytest.UsageError(f"Invalid json format in project config '{path}'")
            except (KeyError, ValueError)as e:
                raise pytest.UsageError(f"Invalid json data in project config '{path}': {e}")


@dataclass
class RemoteWorkerConfig:
    remote_host: str
    arguments: Dict[str, str] = field(default_factory=dict)
    remote_root: Optional[Path] = None
    _http_session = None


    @classmethod
    async def from_raw_list(cls, values: Iterable[str]) -> AsyncGenerator["RemoteWorkerConfig", "RemoteWorkerConfig"]:
        direct_list = []
        http_end_points = []
        file_list = []
        count = 0
        for v in values:
            if v.startswith('https://') or v.startswith('http://'):
                http_end_points.append(v)
            elif v.startswith('file://'):
                file_list.append(v)
            elif Path(v).exists():
                file_list.append(f"file://{v}")
            else:
                direct_list.append(v)
        if direct_list:
            for worker_config in cls.from_list(direct_list):
                yield worker_config
                count += 1
        if file_list:
            for file_uri in file_list:
                async for worker_config in cls.from_uri_string(file_uri):
                    yield worker_config
                    count += 1
        if http_end_points:
            for http_uri in http_end_points:
                try:
                    always_print(f"Attempting to read remote worker configuration from {http_uri}...")
                    cls._http_session = HTTPSession.create(http_uri)
                except Exception as e:
                    always_print(f"Failed to reach end point {http_uri}: {e}; attempting next fallback if present")
                    continue
                try:
                    async for json_data in cls._http_session.start():
                        yield RemoteWorkerConfig(remote_host=json_data['host'],
                                                 arguments=json_data['arguments'],
                                                 remote_root=json_data.get('remote_root'))
                        count += 1
                except Exception as e:
                    always_print(f"Failed to call to start session at {cls._http_session._start_url};"
                                 " attempting next fallback if present")
                    continue
                if count > 0:
                    break
                else:
                    with suppress(Exception):
                        await cls._http_session.end_session()
                    cls._http_session = None
                    always_print(f"Failed to find available workers from {http_uri}; "
                                 "attempting next fallback if present")
        if count == 0:
            pytest.fail("Failed to yield any viable remote workers. aborting")

    @classmethod
    def from_list(cls, value: Iterable[str]) -> List["RemoteWorkerConfig"]:
        """
        parse a cli input string for remote host and parameters specifications
        :param value: a string spec, a str path to a file in form file://..., or to a url in form 'http[s]://...'
            (the latter two returning a json dict in correspondence with return)
        :return: dictionary keyed on host of a dictionary of key value parameters
        """
        configs: List["RemoteWorkerConfig"] = []
        try:
            for item in value:
                host, *args = item.split(';')
                configs.append(RemoteWorkerConfig(
                    remote_host=host,
                    arguments={k: v for k, v in [arg.split('=') for arg in args]}
                ))
        except ValueError:
            raise pytest.UsageError(f"invalid cli input for remote hosts: {value}")
        return configs

    @classmethod
    async def from_uri_string(cls, value: str) -> AsyncGenerator["RemoteWorkerConfig", "RemoteWorkerConfig"]:
        """
        parse a cli input string for remote host and parameters specifications
        :param value: a string spec, a str path to a file in form file://..., or to a url in form 'http[s]://...'
            (the latter two returning a json dict in correspondence with return)
        :return: dictionary keyed on host of a dictionary of key value parameters
        """
        def validate(json_data: Dict[str, Any]):
            if type(json_data) != dict:
                raise pytest.UsageError(
                    f"Invalid json format for client host/args specification from '{value}'"
                )
            if 'host' not in json_data:
                raise pytest.UsageError("'host' not found in provided json")
            if 'arguments' not in json_data:
                raise pytest.UsageError("'arguments' not found in provided json")
            if type(json_data['host']) != str:
                raise pytest.UsageError(
                    f"Invalid json format;  host is not a string name"
                )
            if type(json_data['arguments']) != dict:
                raise pytest.UsageError(
                    f"Invalid json format for client host/args specification from '{value}'")
            for k, v in json_data['arguments'].items():
                if type(k) != str:
                    raise pytest.UsageError(
                        f"Invalid json format for client host/args specification '{value}'")
                if type(v) not in (str, bool, int, float):
                    raise pytest.UsageError(
                        f"Invalid json format for client host/args specification '{value}'")
        try:
            assert value.startswith('file://')
            async with async_open(value[7:], "r") as afp:
                async for line in afp:
                    if line.startswith('#') or not line.strip():
                        continue
                    json_data = json.loads(line.strip())
                    validate(json_data)
                    for k, v in json_data['arguments'].items():
                        json_data['arguments'][k] = str(v)
                    yield RemoteWorkerConfig(remote_host=json_data['host'],
                                             arguments=json_data['arguments'])
        except json.JSONDecodeError:
            raise pytest.UsageError(f"invalid json input from source '{value}'")

    def argument_env_list(self) -> Tuple[List[str], Dict[str, str]]:
        arg_list: List[str] = []
        env_list: Dict[str, str] = {}
        for name, value in self.arguments.items():
            if name.upper() == name:
                env_list[name] = f"\"{str(value)}\""
            else:
                arg_list.append(f"--{name}")
                arg_list.append(f"\"{str(value)}\"")
        return arg_list, env_list

    @classmethod
    def http_session(cls):
        return cls._http_session


@dataclass
class PytestMprocConfig:
    num_cores: int = 1
    server_uri: Optional[str] = 'localhot:None'
    client_connect: Optional[str] = None
    connection_timeout: int = 30
    remote_hosts: List[RemoteWorkerConfig] = field(default_factory=list)
    max_simultaneous_connections = 24
    remote_sys_executable: Optional[Path] = None


@dataclass
class PytestMprocWorkerRuntime:
    @classmethod
    def is_worker(cls):
        return True

    coordinator: Optional["pytest_mproc.coordinator.Coordinator"] = None


@dataclass
class PytestMprocRuntime:

    def is_worker(self):
        return self.mproc_main is None

    mproc_main: Optional["pytest_mproc.main.Orchestrator"] = None
    coordinator: Optional["pytest_mproc.coordinator.Coordinator"] = None
