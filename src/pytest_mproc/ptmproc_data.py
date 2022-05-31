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
TEST_FILES = 'test_files'


@dataclass
class ProjectConfig:
    project_name: str
    project_root: Path
    test_files: List[Path]
    src_paths: List[Path] = field(default_factory=list)
    prepare_script: Optional[Path] = None
    finalize_script: Optional[Path] = None

    @classmethod
    def from_file(cls, path: Path) -> "ProjectConfig":
        with open(path, 'rb') as in_stream:
            try:
                data = json.load(in_stream)
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
                if SRC_PATHS in data and not isinstance(data[SRC_PATHS], Iterable):
                    raise pytest.UsageError(f"'{SRC_PATHS}' in project config '{path}' is not a list of string paths")
                for p in data[SRC_PATHS]:
                    if not (path.parent / p).exists():
                        raise pytest.UsageError(f"source path '{p}' relative to project config "
                                                f"{str(path)} is not a path")
                return ProjectConfig(
                    project_name=data['project_name'],
                    project_root=path.parent,
                    test_files=[Path(p) for p in data[TEST_FILES]],
                    src_paths=[Path(p) for p in data[SRC_PATHS]],
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
        :param value: a string spec, a str path to a file in form file://..., or to a url in form 'http[s]://...'
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
    server_uri: Optional[str] = 'localhot:None'
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

    coordinator: Optional["pytest_mproc.coordinator.Coordinator"] = None


@dataclass
class PytestMprocRuntime:

    def is_worker(self):
        return self.mproc_main is None

    mproc_main: Optional["pytest_mproc.main.Orchestrator"] = None
    coordinator: Optional["pytest_mproc.coordinator.Coordinator"] = None
