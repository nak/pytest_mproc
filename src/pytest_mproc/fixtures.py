import inspect
import functools
import os
import pytest
import sys
from multiprocessing import current_process
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Tuple, Optional
from pytest_mproc import plugin

import logging
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
        os.write(sys.stderr.fileno(), f"Starting {self.__class__.__qualname__} server...\n".encode('utf-8'))
        # server:
        self._fixtures: Dict[str, Any] = {}
        self.__class__.register("get_fixture", self._get_fixture)
        self.__class__.register("put_fixture", self._put_fixture)
        super().start(*args, **kwargs)
        os.write(sys.stderr.fileno(), f"Started {self.__class__.__qualname__} server\n".encode('utf-8'))

    def connect(self):
        os.write(sys.stderr.fileno(), f"Connecting {self.__class__.__name__} server...\n".encode('utf-8'))
        self.__class__.register("get_fixture")
        self.__class__.register("put_fixture")
        super().connect()
        os.write(sys.stderr.fileno(), f"Connected {self.__class__.__name__} server\n".encode('utf-8'))

    def _put_fixture(self, name: str, value: Any) -> None:
        self._fixtures[name] = value

    def _get_fixture(self, name: str) -> Any:
        if name not in self._fixtures:
            return self.Value(FixtureManager.NONE)
        result = self._fixtures.get(name)
        return self.Value(result)


class Node:

    class Manager(FixtureManager):

        def __init__(self, as_main: bool, port: int, name: str = "Node.Manager", connection_timeout:int = 30):
            super().__init__(("127.0.0.1", port))
            if not as_main:
                os.write(sys.stderr.fileno(), f"Connected [{name}]\n".encode('utf-8'))

        PORT = 7038
        _singleton = None

        @classmethod
        def singleton(cls) -> "Node.Manager":
            if cls._singleton is None:
                if "PYTEST_WORKER" in os.environ:
                    cls._singleton = cls(as_main=False, port=cls.PORT)
                    cls._singleton.connect()
                    cls._singleton._is_serving = False
                else:
                    try:
                        cls._singleton = cls(as_main=False, port=cls.PORT, connection_timeout=10)
                        cls._singleton.connect()
                        cls._singleton._is_serving = False
                    except (OSError, EOFError):
                        cls._singleton = cls(as_main=True, port=cls.PORT)
                        cls._singleton.start()
                        cls._singleton._is_serving = True
                    except Exception as e:
                        pytest.exit(f"FAILED TO START NODE MANAGER")
            return cls._singleton

        @classmethod
        def shutdown(cls) -> None:
            if cls._singleton is not None and cls._singleton._is_serving:
                cls._singleton.shutdown()
                cls._singleton = None


class Global:

    class Manager(FixtureManager):

        PORT = 8039
        _singleton = None

        def __init__(self, as_main: bool, host: str, port: int, name: str = "Global.Manager"):
            super().__init__((host, port))

        @classmethod
        def singleton(cls, host: str, port: int) -> "Global.Manager":
            if cls._singleton is None:
                assert host is not None and port is not None, "Internal error: host and port not provided for global manager"
                if "PYTEST_WORKER" in os.environ:
                    cls._singleton = cls(as_main=False, host=host, port=cls.PORT)
                    cls._singleton.connect()
                    cls._singleton._is_serving = False
                else:
                    try:
                        cls._singleton = cls(as_main=False, host=host, port=cls.PORT)
                        cls._singleton.connect()
                        cls._singleton._is_serving = False
                    except (EOFError, OSError):
                        os.write(sys.stderr.fileno(),
                                 b"Failed to connect, trying to start server as not server present...\n")
                        cls._singleton = cls(as_main=True, host=host, port=port)
                        cls._singleton.start()
                        cls._singleton._is_serving = True
                    except Exception as e:
                        import traceback
                        pytest.exit(f"Caught exception {e} \n{traceback.format_exc()}")
            return cls._singleton

        @classmethod
        def shutdown(cls) -> None:
            if cls._singleton is not None and cls._singleton._is_serving:
                cls._singleton.shutdown()
                cls._singleton = None

    def __init__(self, config):
        super().__init__(config)


def global_fixture(host: str, port: Optional[int] = None, **kwargs):
    if 'scope' in kwargs:
        raise pytest.UsageError("Cannot specify scope for 'glboal' fixtures; they are always mapped to 'session'")
    if "--cores" not in sys.argv:
        return pytest.fixture(scope='session', **kwargs)
    global_mgr = Global.Manager.singleton(host, port or Global.Manager.PORT)
    value = None

    def _decorator(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            nonlocal value
            value = value or global_mgr.get_fixture(func.__name__).value()
            if type(value) == FixtureManager.NoneValue:
                v = func(*args, **kwargs)
                if inspect.isgenerator(v):
                    try:
                        value = next(v)
                    except StopIteration:
                        raise Exception("Generator did not yield")
                else:
                    value = v
                global_mgr.put_fixture(func.__name__, value)
            return value

        return pytest.fixture(scope='session', **kwargs)(_wrapper)

    return _decorator


def node_fixture(**kwargs):
    if 'scope' in kwargs:
        raise pytest.UsageError("Cannot specify scope for 'glboal' fixtures; they are always mapped to 'session'")
    if "--cores" not in sys.argv:
        return pytest.fixture(scope='session', **kwargs)
    node_mgr = Node.Manager.singleton()

    def _decorator(func):
        value = None
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            nonlocal value
            value = value or node_mgr.get_fixture(func.name).value()
            if type(value) == FixtureManager.NoneValue:
                v = func(*args, **kwargs)
                if inspect.isgenerator(v):
                    try:
                        value = next(v)
                    except StopIteration:
                        raise Exception("Generator did not yield")
                else:
                    value = v
                node_mgr.put_fixture(func.name, value)
            return value

        return pytest.fixture(scope='session', **kwargs)(_wrapper)

    return _decorator
