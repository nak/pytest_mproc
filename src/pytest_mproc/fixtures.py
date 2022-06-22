import hashlib
import inspect
import functools
import logging
import os
import sys
from contextlib import suppress
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Tuple, Optional
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc import find_free_port, get_auth_key_hex, get_auth_key

# assert plugin  # import takes care of some things on import, but not used otherwise; here to make flake8 happy


logging.basicConfig(level=logging.DEBUG)


class FixtureManager(BaseManager):

    class NoneValue:
        pass

    NONE = NoneValue()

    class Value:

        def __init__(self, val: Any):
            self._val = val

        def value(self) -> Any:
            return self._val

    def __init__(self, addr: Tuple[str, int]):
        super().__init__(address=addr, authkey=get_auth_key())

    # noinspection PyAttributeOutsideInit
    def start(self, **kwargs):
        always_print(f"Starting {self.__class__.__qualname__} server {self.address}...")
        # server:
        self._fixtures: Dict[str, Any] = {}
        self.__class__.register("get_fixture", self._get_fixture)
        self.__class__.register("put_fixture", self._put_fixture)
        super().start(**kwargs)
        always_print(f"Started {self.__class__.__qualname__} server.")

    def connect(self):
        self.__class__.register("get_fixture")
        self.__class__.register("put_fixture")
        debug_print(f"Connecting {self.__class__.__qualname__} server {self.address}...")
        super().connect()
        debug_print(f"Connected {self.__class__.__qualname__} server.")

    def _put_fixture(self, name: str, value: Any) -> None:
        self._fixtures[name] = value

    def _get_fixture(self, name: str) -> Any:
        if name not in self._fixtures:
            return self.Value(FixtureManager.NONE)
        result = self._fixtures.get(name)
        return self.Value(result)


class Node:

    class Manager(FixtureManager):

        PORT = int(os.environ.get('PTMPROC_NODE_MGR_PORT', find_free_port()))
        _singleton = None

        def __init__(self, as_main: bool, port: int, name: str = "Node.Manager"):
            super().__init__(("127.0.0.1", port))
            if not as_main:
                debug_print(f"Connected [{name}]")

        @classmethod
        def singleton(cls) -> "Node.Manager":
            if cls._singleton is None:
                # noinspection PyBroadException
                try:
                    cls._singleton = cls(as_main=False, port=cls.PORT)
                    cls._singleton.connect()
                    cls._singleton._is_serving = False
                except (OSError, EOFError):
                    debug_print(f"Looks like no node manager already running, starting ...")
                    cls._singleton = cls(as_main=True, port=cls.PORT)
                    cls._singleton.start()
                    cls._singleton._is_serving = True
                except Exception as e:
                    raise SystemError(f"FAILED TO START NODE MANAGER") from e
            return cls._singleton

        @classmethod
        def shutdown(cls) -> None:
            # noinspection PyProtectedMember
            if cls._singleton is not None and cls._singleton._is_serving:
                cls._singleton.shutdown()
                cls._singleton = None


class Global:

    class Manager(FixtureManager):

        _singleton: Dict[str, "Global.Manager"] = {}

        def __init__(self, host: str, port: int):
            super().__init__((host, port))

        @property
        def port(self):
            return self.address[1]

        @property
        def host(self):
            return self.address[0]

        @property
        def uri(self):
            return f"{self.address[0]}:{self.address[1]}"

        @staticmethod
        def key() -> str:
            return hashlib.sha256(get_auth_key()).hexdigest()

        @classmethod
        def singleton(cls, address: Optional[Tuple[str, int]] = None, as_client: bool = False) -> "Global.Manager":
            if cls.key() in cls._singleton and (address is None or address == cls._singleton[cls.key()].address):
                return cls._singleton[cls.key()]
            elif address is None:
                raise SystemError("Attempt to get Global manager before start or connect")
            if as_client:
                host, port = address
                host = host.split('@', maxsplit=1)[-1]
                assert host is not None and port is not None, \
                    "Internal error: host and port not provided for global manager"
                singleton = cls(host=host, port=port)
                singleton.connect()
                singleton._is_serving = False
            else:
                host, port = address
                host = host.split('@', maxsplit=1)[-1]
                singleton = cls(host=host, port=port)
                singleton.start()
                singleton._is_serving = True
            cls._singleton[cls.key()] = singleton
            return cls._singleton[cls.key()]

        @classmethod
        def stop(cls) -> None:
            if cls.key() in cls._singleton:
                with suppress(Exception):  # shutdown only available if server and not client
                    cls._singleton[cls.key()].shutdown()
                del cls._singleton[cls.key()]

    def __init__(self, config):
        super().__init__(config)


def global_fixture(**kwargs):
    import pytest
    if 'scope' in kwargs:
        raise pytest.UsageError("Cannot specify scope for 'glboal' fixtures; they are always mapped to 'session'")
    if "--cores" not in sys.argv:
        return pytest.fixture(scope='session', **kwargs)
    value = None

    def _decorator(func):

        # noinspection DuplicatedCode
        @functools.wraps(func)
        def _wrapper(*args, **kwargs_):
            nonlocal value
            # TODO: make this a "is_present" call in Global.Manager
            assert Global.Manager.key() in Global.Manager._singleton, "Global Manager did not start as expected"
            global_mgr = Global.Manager.singleton()
            # when serving potentially multiple full pytest session in a standalone server,
            # we distinguish sessions via the auth token
            # noinspection PyUnresolvedReferences
            key = f"{get_auth_key_hex()} {func.__name__}"
            # noinspection PyUnresolvedReferences
            value = value or global_mgr.get_fixture(key).value()
            if type(value) == FixtureManager.NoneValue:
                v = func(*args, **kwargs_)
                if inspect.isgenerator(v):
                    try:
                        value = next(v)
                    except StopIteration:
                        raise Exception("Generator did not yield")
                else:
                    value = v
                # noinspection PyUnresolvedReferences
                global_mgr.put_fixture(key, value)
            return value

        return pytest.fixture(scope='session', **kwargs)(_wrapper)

    return _decorator


def node_fixture(**kwargs):
    import pytest
    if 'scope' in kwargs:
        raise pytest.UsageError("Cannot specify scope for 'glboal' fixtures; they are always mapped to 'session'")
    if "--cores" not in sys.argv:
        return pytest.fixture(scope='session', **kwargs)

    def _decorator(func):
        value = None

        # noinspection DuplicatedCode
        @functools.wraps(func)
        def _wrapper(*args, **kwargs_):
            nonlocal value
            node_mgr = Node.Manager.singleton()
            func_name = func.__name__
            # noinspection PyUnresolvedReferences
            value = value or node_mgr.get_fixture(func_name).value()
            if type(value) == FixtureManager.NoneValue:
                v = func(*args, **kwargs_)
                if inspect.isgenerator(v):
                    try:
                        value = next(v)
                    except StopIteration:
                        raise Exception("Generator did not yield")
                else:
                    value = v
                # noinspection PyUnresolvedReferences
                node_mgr.put_fixture(func_name, value)
            return value

        return pytest.fixture(scope='session', **kwargs)(_wrapper)

    return _decorator
