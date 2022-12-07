import inspect
import functools
import logging
from contextlib import suppress
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Tuple, Optional

from pytest_mproc.user_output import debug_print, always_print

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

    def __init__(self, addr: Tuple[str, int], authkey: bytes):
        super().__init__(address=addr, authkey=authkey)

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

        _singleton: Optional["Node.Manager"] = None
        _port: int = 0

        def __init__(self, as_main: bool,  authkey: bytes, port: int = 0, name: str = "Node.Manager"):
            if self.__class__._port == 0:
                raise ValueError("Request for Node.Manager singleton but none instantiated")
            super().__init__(("127.0.0.1", port), authkey)
            if not as_main:
                debug_print(f"Connected [{name}]")
            self._is_serving = as_main
            self._port = port

        @property
        def port(self):
            return self._port

        @classmethod
        def singleton(cls, node_mgr_port: int = 0, authkey: Optional[bytes] = None) -> "Node.Manager":
            if cls._port != 0 and node_mgr_port != cls._port:
                raise ValueError(f"Inconsistent port provided, not matching previous: {cls._port} != {node_mgr_port}")
            if cls._singleton:
                return cls._singleton
            cls._port = node_mgr_port
            # noinspection PyBroadException
            try:
                cls._singleton = cls(as_main=False, port=cls._port, authkey=authkey)
                cls._singleton.connect()
            except (OSError, EOFError):
                if cls._port == 0:
                    raise ValueError("Request for Node.Manager singleton but none instantiated")
                debug_print(f"Looks like no node manager already running, starting ...")
                cls._singleton = cls(as_main=True, port=cls._port, authkey=authkey)
                cls._singleton.start()
            except Exception as e:
                raise SystemError(f"FAILED TO START NODE MANAGER") from e
            return cls._singleton

        def shutdown(self) -> None:
            # noinspection PyProtectedMember
            super().shutdown()
            Node.Manager._singleton = None

        def stop(self) -> None:
            with suppress(Exception):
                super().shutdown()
            Node.Manager._singleton = None


class Global:

    class Manager(FixtureManager):

        # We track singleton based on pid, as a multiprocessing.Process can carry over a singleton into
        # that new process when forked.  Always use the singleton() method to access the singleon, as this
        # does the propoer check on matching pid
        _singleton: Optional["Global.Manager"] = None

        def __init__(self, host: str, port: int, authkey: bytes):
            super().__init__((host, port), authkey=authkey)

        @property
        def port(self):
            return self.address[1]

        @property
        def host(self):
            return self.address[0]

        @property
        def uri(self):
            return f"{self.address[0]}:{self.address[1]}"

        @classmethod
        def singleton(cls) -> Optional["Global.Manager"]:
            return cls._singleton

        @classmethod
        def as_server(cls, address: Tuple[str, int], auth_key: bytes) -> "Global.Manager":
            """
            :return: already established singleton, or create the singleton as a server and return
            """
            if cls._singleton is not None:
                raise RuntimeError(f"Attempt to create global fixture manager twice")
            host, port = address
            mgr = cls(host=host, port=port, authkey=auth_key)
            mgr.start()
            mgr._is_serving = True
            cls._singleton = mgr
            return cls.singleton()

        @classmethod
        def as_client(cls, address: Tuple[str, int], auth_key: bytes) -> "Global.Manager":
            """
            :return: already established singleton, or create the singleton as a client and return
            """
            if cls._singleton:
                return cls._singleton
            host, port = address
            host = host.split('@', maxsplit=1)[-1]
            assert host is not None and port is not None, \
                "Internal error: host and port not provided for global manager"
            mgr = cls(host=host, port=port, authkey=auth_key)
            mgr.connect()
            mgr._is_serving = False
            cls._singleton = mgr
            return cls.singleton()

        def stop(self) -> None:
            with suppress(Exception):
                super().shutdown()
            Global.Manager._singleton = None

    def __init__(self, config):
        super().__init__(config)


def global_fixture(func_=None, **kwargs):
    import pytest
    if 'scope' in kwargs:
        raise pytest.UsageError("Cannot specify scope for 'global' fixtures; they are always mapped to 'session'")
    if 'autouse' in kwargs:
        raise pytest.UsageError("Cannot specify 'autouse' for global-scoped fixtures")
    value = None

    def _decorator(func=func_, **kwargs_):
        # noinspection DuplicatedCode
        @functools.wraps(func)
        def _wrapper(*args, **kwargs__):
            nonlocal value
            global_mgr = Global.Manager.singleton()
            if global_mgr is None:
                raise RuntimeError("Global fixture manager not started as expected")
            # when serving potentially multiple full pytest session in a standalone server,
            # we distinguish sessions via the auth token
            # noinspection PyUnresolvedReferences
            key = f"{func.__name__}"
            # noinspection PyUnresolvedReferences
            value = value or global_mgr.get_fixture(key).value()
            if type(value) == FixtureManager.NoneValue:
                v = func(*args, **kwargs__)
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
        if Global.Manager.singleton() is None:
            return pytest.fixture(scope='session', **kwargs_)(func)
        else:
            return pytest.fixture(scope='session', **kwargs_)(_wrapper)

    if func_ is None:
        def functional(func__):
            return _decorator(func__, **kwargs)
        return functional
    return _decorator(func_)


def node_fixture(func=None, **kwargs):
    import pytest
    if 'scope' in kwargs:
        raise pytest.UsageError("Cannot specify scope for 'glboal' fixtures; they are always mapped to 'session'")
    if 'autouse' in kwargs:
        raise pytest.UsageError("Cannot specify 'autouse' for node-scoped fixtures")
    if func is not None:
        value = None

        @functools.wraps(func)
        def _wrapper(*args, **kwargs_):
            nonlocal value
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

        # noinspection PyProtectedMember
        if Node.Manager._port != 0:
            # noinspection PyProtectedMember
            node_mgr = Node.Manager.singleton(Node.Manager._port)
            return pytest.fixture(scope='session', **kwargs)(_wrapper)
        else:
            return pytest.fixture(scope='session', **kwargs)(func)
    else:
        def functional(func__):
            assert func__ is not None
            return node_fixture(func__, **kwargs)
        return functional
