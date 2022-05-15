import os

import aiohttp
import json
import pytest
import pytest_mproc

from aiofile import async_open
from pathlib import Path
from typing import Optional, Dict, List, Iterable, Tuple, AsyncGenerator, Any
from dataclasses import dataclass, field

SRC_PATHS = 'src_paths'
REQUIREMENTS_PATHS = 'requirements_paths'
PURE_REQUIREMENTS_PATHS = 'pure_requirements_paths'
TESTS_PATH = 'tests_path'
RESOURCE_PATHS = 'resource_paths'


@dataclass
class ProjectConfig:
    tests_path: Path
    requirements_paths: List[Path] = field(default_factory=list)
    pure_requirements_paths: List[Path] = field(default_factory=list)
    src_paths: List[Path] = field(default_factory=list)
    resource_paths: List[Tuple[Path, Path]] = field(default_factory=list)
    prepare_script: Optional[Path] = None
    finalize_script: Optional[Path] = None

    @classmethod
    def from_file(cls, path: Path) -> "ProjectConfig":
        with open(path, 'rb') as in_stream:
            try:
                data = json.load(in_stream)
                if 'requirements_paths' in data and\
                        not isinstance(data.get(REQUIREMENTS_PATHS), Iterable):
                    raise pytest.UsageError(f"requirements_paths in project config '{path}' is either missing or not "
                                            " a list of string paths")
                if 'pure_requirements_paths' in data and\
                        not isinstance(data.get(PURE_REQUIREMENTS_PATHS), Iterable):
                    raise pytest.UsageError(f"requirements_paths in project config '{path}' is either missing or not "
                                            " a list of string paths")
                if 'finalize_script' in data and (not Path(data['finalize_script']).exists or
                                                  not os.access(data['finalize_script'], os.X_OK)):
                    raise pytest.UsageError(
                        f"finalize script {data['finalize_script']} either does not exist or is not executable")
                if 'prepare_script' in data and (not Path(data['prepare_script']).exists or
                                                 not os.access(data['prepare_script'], os.X_OK)):
                    raise pytest.UsageError(
                        f"prepare script {data['prepare_script']} either does not exist or is not executable")
                if TESTS_PATH not in data:
                    raise pytest.UsageError(f"'{TESTS_PATH}' is not in project config '{path}'")
                if type(data[TESTS_PATH]) != str:
                    raise pytest.UsageError(f"'test_path' must be a single string path in '{path}'")
                data[TESTS_PATH] = path.parent / data[TESTS_PATH] \
                    if not Path(data[TESTS_PATH]).is_absolute() else data[TESTS_PATH]
                if not Path(data[TESTS_PATH]).exists():
                    raise pytest.UsageError(f"'{TESTS_PATH}' {data[TESTS_PATH]}' does not exist as specified"
                                            f" in project config {path}")
                if SRC_PATHS in data and not isinstance(data[SRC_PATHS], Iterable):
                    raise pytest.UsageError(f"'{SRC_PATHS}' in project config '{path}' is not a list of string paths")
                if RESOURCE_PATHS in data and not isinstance(data[RESOURCE_PATHS], Iterable):
                    raise pytest.UsageError(f"'{RESOURCE_PATHS}' in project config '{path}' is not a list of string paths")
                data[REQUIREMENTS_PATHS] = [path.parent / str(p) if not Path(p).is_absolute() else Path(p)
                                            for p in data.get(REQUIREMENTS_PATHS,[])]
                data[PURE_REQUIREMENTS_PATHS] = [path.parent / str(p) if not Path(p).is_absolute() else Path(p)
                                                 for p in data.get(PURE_REQUIREMENTS_PATHS, [])]
                for p in data[REQUIREMENTS_PATHS] + data[PURE_REQUIREMENTS_PATHS]:
                    if not Path(p).exists():
                        raise pytest.UsageError(f"requirements path {p} in project config is not a path")
                data[SRC_PATHS] = [path.parent / str(p) if not Path(p).is_absolute() else Path(p)
                                     for p in data[SRC_PATHS]]
                for p in data[SRC_PATHS]:
                    if not Path(p).exists():
                        raise pytest.UsageError(f"source path {p} in project config is not a path")
                data[RESOURCE_PATHS] = [path.parent / str(p) if not Path(p).is_absolute() else Path(p)
                                          for p in data[RESOURCE_PATHS]]
                for p in data[RESOURCE_PATHS]:
                    if not Path(p).exists():
                        raise pytest.UsageError(f"resource path {p} in project config is not a path")
                return ProjectConfig(
                    requirements_paths=[Path(p) for p in data[REQUIREMENTS_PATHS]],
                    pure_requirements_paths=data[PURE_REQUIREMENTS_PATHS],
                    tests_path=Path(data[TESTS_PATH]),
                    src_paths=[Path(p) for p in data[SRC_PATHS]],
                    resource_paths=[Path(p) for p in data[RESOURCE_PATHS]],
                    prepare_script=data.get('prepare_script'),
                    finalize_script=data.get('finalize_script'),
                )
            except json.JSONDecodeError:
                raise pytest.UsageError(f"Invalid json format in project config '{path}'")
            except (KeyError, ValueError)as e:
                raise pytest.UsageError(f"Invalid json data in project config '{path}': {e}")


@dataclass
class RemoteHostConfig:
    remote_host: str
    arguments: Dict[str, str] = field(default_factory=dict)
    remote_root: Optional[Path] = None

    @classmethod
    def from_list(cls, value: Iterable[str]) -> List["RemoteHostConfig"]:
        """
        parse a cli input string for remote host and parameters specifications
        :param value: a string specification, a str path to a file in form file://..., or to a url in form 'http[s]://...'
            (the latter two returning a json dict in correspondence with return)
        :return: dictionary keyed on host of a dictionary of key value parameters
        """
        configs: List["RemoteHostConfig"] = []
        try:
            for item in value:
                host, *args = item.split(';')
                configs.append(RemoteHostConfig(
                    remote_host=host,
                    arguments={k: v for k, v in [arg.split('=') for arg in args]}
                ))
        except ValueError:
            raise pytest.UsageError(f"invalid cli input for remote hosts: {value}")
        return configs

    @classmethod
    async def from_uri_string(cls, value: str) -> AsyncGenerator["RemoteHostConfig", "RemoteHostConfig"]:
        """
        parse a cli input string for remote host and parameters specifications
        :param value: a string specification, a str path to a file in form file://..., or to a url in form 'http[s]://...'
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
            if value.startswith('http://') or value.startswith('https://'):
                async with aiohttp.ClientSession() as session:
                    async with session.get(value) as resp:
                        async for data, _ in resp.content.iter_chunks():
                            buffer += data
                            if '\n' in data:
                                *lines, buffer = data.split('\n')
                                for line in lines:
                                    if not line.strip():
                                        continue
                                    json_data = json.loads(line.strip())
                                    validate(json_data)
                                    for k, v in json_data['arguments'].items():
                                        json_data['arguments'][k] = str(v)
                                    yield RemoteHostConfig(remote_host=json_data['host'],
                                                           arguments=json_data['arguments'])
            elif value.startswith('file://'):
                async with async_open(value[7:], "r") as afp:
                    async for line in afp:
                        if line.startswith('#') or not line.strip():
                            continue
                        json_data = json.loads(line.strip())
                        validate(json_data)
                        for k, v in json_data['arguments'].items():
                            json_data['arguments'][k] = str(v)
                        yield RemoteHostConfig(remote_host=json_data['host'],
                                                arguments=json_data['arguments'])
            else:
                raise pytest.UsageError(f"Invalid value for remote host(s): {value}")
        except json.JSONDecodeError:
            raise pytest.UsageError(f"invalid json input from source '{value}'")
        except ValueError:
            raise pytest.UsageError(f"invalid cli input for remote hosts: {value}")

    def argument_env_list(self) -> Tuple[List[str], Dict[str, str]]:
        arg_list: List[str] = []
        env_list: Dict[str, str] = {}
        for name, value in self.arguments.items():
            if name.upper() == name:
                env_list[name] = value
            else:
                arg_list.append(f"--{name}")
                arg_list.append(str(value))
        return arg_list, env_list


@dataclass
class PytestMprocConfig:
    num_cores: int = 1
    server_port: Optional[int] = None
    server_host: Optional[str] = None
    client_connect: Optional[str] = None
    connection_timeout: int = 30
    remote_hosts: List[RemoteHostConfig] = field(default_factory=list)
    max_simultaneous_connections = 24
    remote_sys_executable: Optional[Path] = None


@dataclass
class PytestMprocWorkerRuntime:
    @classmethod
    def is_worker(cls):
        return True

    #worker: Optional["pytest_mproc.worker.WorkerSession"] = None
    coordinator: Optional["pytest_mproc.coordinator.Coordinator"] = None


@dataclass
class PytestMprocRuntime:

    def is_worker(self):
        return self.mproc_main is None

    mproc_main: Optional["pytest_mproc.main.Orchestrator"] = None
    coordinator: Optional["pytest_mproc.coordinator.Coordinator"] = None
