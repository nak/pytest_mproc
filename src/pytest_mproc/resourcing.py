import binascii
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Tuple, Dict, List

import aiofile

from pytest_mproc.user_output import always_print


@dataclass
class RemoteWorkerNode:
    resource_id: str
    address: Tuple[str, int]
    addl_params: Dict[str, str]
    env: Dict[str, str]
    flags: List[str]
    authkey: bytes

    @classmethod
    def from_json(cls, json_text: str):
        """
        :return RemoteNode instance from provided json string
           json format should be:
             {'resource_id': <id of resource, unique to session>,
              'host': <hostname of node>,
              'port': <port of node>,
              'addl_params': <dict based on str-key where any key in all caps will be treated as environment var to be
                 set for worker, and all others will be treated as command line potion to pytest (--<key> <value>)>
              'flags': name of flags to be added to pytest cmd line (will be translated to --<flag-name>, aka no need
                       for '--' in name)
              }
            DO NOT put '--' in the addl_params or flags names
        """
        json_data = json.loads(json_text)
        return RemoteWorkerNode(
            resource_id=json_data['worker_id'],
            address=(json_data['host'], int(json_data['port'])),
            addl_params={key: str(value) for key, value in json_data['addl_params'].items() if key.upper() != key},
            env={key: str(value) for key, value in json_data['addl_params'] if key.upper() == key},
            flags=json_data['flags'],
            authkey=json_data['authkey']
        )


class ResourceManager(ABC):

    _implementations = {}

    # noinspection PyUnusedLocal
    @abstractmethod
    async def reserve(self, count: int) -> AsyncIterator[RemoteWorkerNode]:
        """
        :param count: how many resources to reserve
        :return: an async iterator, yielding with RemoteNode as they become available.  For systems that require
          additional preparation time, there may be a gap in time between yield of each node based on this prep time
          NOTE: the worker id return in each RemoteWorkerNode must be unique to the session
        """
        raise NotImplementedError()
        # noinspection PyUnreachableCode
        yield None

    @abstractmethod
    async def relinquish(self, worker_id: str):
        """
        Relinquish the resource associated with a RemoteWorkerNode that has previously been reserved (via call to
           reserve() )
        :param worker_id: unique string id given for the RemoteWorkerNode upon reservation
        """

    @classmethod
    def register_impl(cls, protocol_name: str, clazz):
        if protocol_name in cls._implementations:
            raise ValueError(f"Protocol {protocol_name} already registered")
        cls._implementations[protocol_name] = clazz

    @classmethod
    def implementation(cls, protocol: str):
        return cls._implementations.get(protocol)


class FixedResourceManager(ResourceManager):
    """
    implementation of ResourceManager, primarily for exploration of use of pytest_mproc in a distributed setting,
    that utilizes a fixed set of host addresses (nodes) for distributing work load
    """

    def __init__(self, uri_path: str):
        path = Path(uri_path)
        if not path.is_file():
            raise ValueError(f"Given file name, '{uri_path}', in URI is not a file")
        self._path = path

    async def reserve(self, worker_count: int) -> AsyncIterator[RemoteWorkerNode]:
        count = 0
        authkey = None
        async with aiofile.async_open(self._path, 'r') as in_stream:
            async for line in in_stream:
                line: str
                if not line.strip() or line.startswith('#'):
                    continue
                if line.startswith('authkey:'):
                    authkey = binascii.a2b_hex(line[8:].strip())
                    continue
                else:
                    if authkey is None:
                        raise RuntimeError(f"No authkey defined in {self._path} as expected")
                    try:
                        address, *args = line.split(',')
                        host, port = address.split(':', maxsplit=1)
                        port = int(port)
                        kwargs = {}
                        env = {}
                        flags = []
                        for arg in args:
                            if '=' not in arg:
                                flags.append(arg)
                            else:
                                k, v = arg.split('=')
                                if k.upper() == k:
                                    env[k] = v
                                else:
                                    kwargs[k] = v
                        count += 1
                        yield RemoteWorkerNode(
                                address=(host, port), addl_params=kwargs, env=env,
                                resource_id=f"resource-{count}",
                                flags=flags,
                                authkey=authkey,
                            )
                        if count == worker_count:
                            break
                    except ValueError:
                        always_print(f"Unable to parse line {line} in {self._path} for address(host/port) and arguments",
                                     as_error=True)

    async def relinquish(self, worker_id: str):
        pass


ResourceManager.register_impl("fixed_hosts", FixedResourceManager)
