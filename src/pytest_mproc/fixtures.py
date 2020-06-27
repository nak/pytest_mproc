import os
import queue
import sys
import time
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Tuple

import _pytest

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

    class Value:

        def __init__(self, val):
            self._val = val

        def value(self):
            return self._val

    def __init__(self, addr: Tuple[str, int], as_main: bool, passw: str):
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
        super().__init__(address=addr, authkey=passw.encode('utf-8'))

        if as_main:
            super().start()
        else:
            tries_remaining = 60
            while tries_remaining:
                try:
                    super().connect()
                    break
                except (ConnectionResetError, ConnectionError, ConnectionAbortedError, ConnectionRefusedError) as e:
                    os.write(sys.stderr.fileno(), b"@")
                    tries_remaining -= 1
                    if tries_remaining == 0:
                        os.write(sys.stderr.fileno(), b"\n")
                        raise Exception(f"Failed to connect to server {main.host} port " +
                                        f"{main.port}: {str(e)}")
                    time.sleep(0.5)
                except TimeoutError:
                    tries_remaining -= 1
                    time.sleep(0.5)


    def get_fixture(self, name: str):
        return self.get_fixture_(name).value()

    def _put_fixture(self, name: str, value: Any):
        self._fixtures[name] = value
        self._fixture_q.put((name, value))

    def _get_fixture(self, name: str):
        result = self._fixtures.get(name)
        while result is None:
            name, value = self._fixture_q.get()
            self._fixtures[name] = value
            result = self._fixtures.get(name)
        return self.Value(result)



class Node(_pytest.main.Session):

    class Manager(FixtureManager):

        def __init__(self, as_main: bool, port: int, name: str="Node.Manager"):
            if not as_main:
                os.write(sys.stderr.fileno(), f"Waiting for server...[{name}]\n".encode('utf-8'))
            super().__init__(("127.0.0.1", port), as_main, passw='pass2')
            if not as_main:
                os.write(sys.stderr.fileno(), f"Connected [{name}]\n".encode('utf-8'))


class Global(_pytest.main.Session):

    def __init__(self, config):
        super().__init__(config)

    # def setup(self):
    #     raise Exception("UNIMPL")

    # def gethookproxy(self, fspath):
    #     raise Exception("UNIMPL")

    # def isinitpath(self, path):
    #     return path in self.session._initialpaths

    # def collect(self):
    #     raise Exception("UNIMPL")


_pytest.fixtures.scopename2class.update({"global": Global, "node": Node})
