import os
import queue
import sys
import time
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Tuple

import _pytest

from pytest_mproc import AUTHKEY

_getscopeitem_orig = _pytest.fixtures.FixtureRequest._getscopeitem


def _getscopeitem_redirect(self, scope):
    if scope == "global" or scope == "node":
        return self._pyfuncitem.getparent(_pytest.main.Session)
    else:
        return _getscopeitem_orig(self, scope)

_pytest.fixtures.scopes.insert(0, "node")
_pytest.fixtures.scopes.insert(0, "global")
_pytest.fixtures.scopenum_function = _pytest.fixtures.scopes.index("function")
_pytest.fixtures.scope2props['global'] = ()
_pytest.fixtures.scope2props['node'] = ()
_pytest.fixtures.FixtureRequest._getscopeitem = _getscopeitem_redirect


class FixtureManager(BaseManager):

    CONNECTION_TIMEOUT = 30  # changeable via pytest cmd line (pytest_cmdline_main impl in plugin.py)

    class Value:

        def __init__(self, val: Any):
            self._val = val

        def value(self) -> Any:
            return self._val

    def __init__(self, addr: Tuple[str, int], as_main: bool):
        if not as_main:
            # client
            self.__class__.register("get_fixture_")
            self.__class__.register("put_fixture")
        else:
            # server:
            self._fixtures: Dict[str, Any] = {}
            self._fixture_q = queue.Queue()
            self.__class__.register("get_fixture_", self._get_fixture)
            self.__class__.register("put_fixture", self._put_fixture)
        super().__init__(address=addr, authkey=AUTHKEY)

        if as_main:
            super().start()
        else:
            chars = ['|', '\\', '-', '/', '|', '-']
            tries_remaining = self.CONNECTION_TIMEOUT*4
            while tries_remaining:
                try:
                    super().connect()
                    break
                except (ConnectionResetError, ConnectionError, ConnectionAbortedError, ConnectionRefusedError) as e:
                    ch = chars[tries_remaining % len(chars)]
                    seconds_left = int(tries_remaining/4)
                    os.write(sys.stderr.fileno(), f"\r{ch} Connecting ... {seconds_left}        ".encode('utf-8'))
                    tries_remaining -= 1
                    if tries_remaining == 0:
                        os.write(sys.stderr.fileno(), b"\n")
                        raise Exception(f"Failed to connect to server {addr[0]} port {addr[1]}: {str(e)}")
                    time.sleep(0.25)
                except TimeoutError:
                    tries_remaining -= 1
                    time.sleep(0.25)

    def get_fixture(self, name: str) -> Value:
        return self.get_fixture_(name).value()

    def _put_fixture(self, name: str, value: Any) -> None:
        self._fixtures[name] = value
        self._fixture_q.put((name, value))

    def _get_fixture(self, name: str) -> Any:
        result = self._fixtures.get(name)
        while result is None:
            name, value = self._fixture_q.get()
            self._fixtures[name] = value
            result = self._fixtures.get(name)

        return self.Value(result)


class Node(_pytest.main.Session):

    class Manager(FixtureManager):

        def __init__(self, as_main: bool, port: int, name: str = "Node.Manager"):
            if not as_main:
                os.write(sys.stderr.fileno(), f"Waiting for server...[{name}]\n".encode('utf-8'))
            super().__init__(("127.0.0.1", port), as_main)
            if not as_main:
                os.write(sys.stderr.fileno(), f"Connected [{name}]\n".encode('utf-8'))


class Global(_pytest.main.Session):

    def __init__(self, config):
        super().__init__(config)


_pytest.fixtures.scopename2class.update({"global": Global, "node": Node})
