import inspect
import functools
import logging
import os
import sys
from multiprocessing import current_process
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Tuple, Optional
from pytest_mproc.user_output import debug_print, always_print
from pytest_mproc import find_free_port

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

    def __init__(self, addr: Tuple[str, int], auth_key: Optional[bytes] = None):
        super().__init__(address=addr, authkey=auth_key or current_process().authkey)

    # noinspection PyAttributeOutsideInit
    def start(self, *args, **kwargs):
        always_print(f"Starting {self.__class__.__qualname__} server {self.address}...")
        # server:
        self._fixtures: Dict[str, Any] = {}
        self.__class__.register("get_fixture", self._get_fixture)
        self.__class__.register("put_fixture", self._put_fixture)
        super().start(*args, **kwargs)
        always_print(f"Started {self.__class__.__qualname__} server")

    def connect(self):
        debug_print(f"Connecting {self.__class__.__qualname__} server {self.address}...")
        self.__class__.register("get_fixture")
        self.__class__.register("put_fixture")
        try:
            super().connect()
        except Exception as e:
            always_print(f"!!! Exception connecting {self.__class__.__qualname__}: {e}")
            raise
        debug_print(f"Connected {self.__class__.__qualname__} server")

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
                if "PYTEST_WORKER" in os.environ:
                    cls._singleton = cls(as_main=False, port=cls.PORT)
                    cls._singleton.connect()
                    cls._singleton._is_serving = False
                else:
                    # noinspection PyBroadException
                    try:
                        cls._singleton = cls(as_main=False, port=cls.PORT)
                        cls._singleton.connect()
                        cls._singleton._is_serving = False
                    except (OSError, EOFError):
                        cls._singleton = cls(as_main=True, port=cls.PORT)
                        cls._singleton.start()
                        cls._singleton._is_serving = True
                    except Exception:
                        raise SystemError(f"FAILED TO START NODE MANAGER")
            return cls._singleton

        @classmethod
        def shutdown(cls) -> None:
            # noinspection PyProtectedMember
            if cls._singleton is not None and cls._singleton._is_serving:
                cls._singleton.shutdown()
                cls._singleton = None


class Global:

    class Manager(FixtureManager):

        _singleton = None

        def __init__(self, host: str, port: int):
            super().__init__((host, port))

        @classmethod
        def singleton(cls, address: Optional[Tuple[str, int]] = None, as_client: bool = False,
                      serve_forever: bool = False) -> "Global.Manager":
            if cls._singleton:
                return cls._singleton
            elif address is None:
                raise SystemError("Attempt to get Global manager before start or connect")
            if as_client:
                host, port = address
                host = host.split('@', maxsplit=1)[-1]
                assert host is not None and port is not None, \
                    "Internal error: host and port not provided for global manager"
                try:
                    cls._singleton = cls(host=host, port=port)
                    cls._singleton.connect()
                    cls._singleton._is_serving = False
                except Exception as e:
                    import traceback
                    raise SystemError(f"Caught exception connecting to global server at {host}:{port}: {e} \n{traceback.format_exc()}")
            else:
                host, port = address
                cls._singleton = cls(host=host, port=port)
                if serve_forever:
                    always_print("Serving remote global manager ...")
                    cls._singleton.get_server().serve_forever()
                else:
                    cls._singleton.start()
                cls._singleton._is_serving = True
            return cls._singleton

        @classmethod
        def shutdown(cls) -> None:
            # noinspection PyProtectedMember
            if cls._singleton is not None and cls._singleton._is_serving:
                cls._singleton.shutdown()
                cls._singleton = None

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
        def _wrapper(*args, **kwargs):
            nonlocal value
            assert Global.Manager._singleton is not None, "Global Manager did not start as expected"
            global_mgr = Global.Manager.singleton()
            # when serving potentially multiple full pytest session in a standalone server,
            # we distinguish sessions via the auth token
            # noinspection PyUnresolvedReferences
            key = f"{current_process().authkey} {func.__name__}"
            value = value or global_mgr.get_fixture(key).value()
            if type(value) == FixtureManager.NoneValue:
                v = func(*args, **kwargs)
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
    node_mgr = Node.Manager.singleton()

    def _decorator(func):
        value = None

        # noinspection DuplicatedCode
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            nonlocal value
            func_name = func.__name__
            # noinspection PyUnresolvedReferences
            value = value or node_mgr.get_fixture(func_name).value()
            if type(value) == FixtureManager.NoneValue:
                v = func(*args, **kwargs)
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
