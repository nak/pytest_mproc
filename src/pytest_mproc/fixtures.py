import os
import queue
import sys
import time
from enum import Enum
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Tuple, Optional, Union

import _pytest
from _pytest import nodes
from _pytest.compat import assert_never

from pytest_mproc import AUTHKEY

if hasattr(_pytest.fixtures, 'scopes'):
    _getscopeitem_orig = _pytest.fixtures.FixtureRequest._getscopeitem


    def _getscopeitem_redirect(self, scope):
        if scope == "global":
            return self._pyfuncitem.getparent(_pytest.main.Session)
        elif scope == 'node':
            return self._pyfuncitem.getparent(_pytest.main.Session)
        else:
            return _getscopeitem_orig(self, scope)

    _pytest.fixtures.scopes.insert(0, "node")
    _pytest.fixtures.scopes.insert(0, "global")
    _pytest.fixtures.scopenum_function = _pytest.fixtures.scopes.index("function")
    _pytest.fixtures.FixtureRequest._getscopeitem = _getscopeitem_redirect

else:
    import _pytest.scope as scope_
    from functools import total_ordering

    @total_ordering
    class DistributedScope(Enum):
        Node: str = "node"
        Global: str = "global"

        @classmethod
        def from_user(
            cls, scope_name: str, descr: str, where: Optional[str] = None
        ) -> Union[scope_.Scope, "DistributedScope"]:
            """
            Given a scope name from the user, return the equivalent Scope enum. Should be used
            whenever we want to convert a user provided scope name to its enum object.

            If the scope name is invalid, construct a user friendly message and call pytest.fail.
            """
            try:
                try:
                    return scope_.Scope(scope_name)
                except ValueError:
                    return DistributedScope(scope_name)
            except ValueError:
                from _pytest.outcomes import fail
                fail(
                    "{} {}got an unexpected scope value '{}'".format(
                        descr, f"from {where} " if where else "", scope_name
                    ),
                    pytrace=False,
                )

        def __lt__(self, other: Union["DistributedScope", scope_.Scope]) -> bool:
            self_index = scope_._SCOPE_INDICES[self]
            other_index = scope_._SCOPE_INDICES[other]
            return self_index < other_index

    scope_.Scope.from_user = DistributedScope.from_user
    scope_._ALL_SCOPES.extend(list(DistributedScope))
    scope_._SCOPE_INDICES.update({sc: len(list(scope_.Scope)) + index for index, sc in enumerate(list(DistributedScope))})
    scope_.HIGH_SCOPES.extend(list(DistributedScope))

    def get_scope_node(
        node: nodes.Node, scope: Union[scope_.Scope, DistributedScope]
    ) -> Optional[Union[nodes.Item, nodes.Collector]]:
        import _pytest.python

        if scope is scope_.Scope.Function:
            return node.getparent(nodes.Item)
        elif scope is scope_.Scope.Class:
            return node.getparent(_pytest.python.Class)
        elif scope is scope_.Scope.Module:
            return node.getparent(_pytest.python.Module)
        elif scope is scope_.Scope.Package:
            return node.getparent(_pytest.python.Package)
        elif scope in [scope_.Scope.Session, DistributedScope.Node, DistributedScope.Global]:
            return node.getparent(_pytest.main.Session)
        else:
            assert_never(scope)
    _pytest.fixtures.get_scope_node = get_scope_node

if hasattr(_pytest.fixtures, 'scope2props'):
    _pytest.fixtures.scope2props['global'] = ()
    _pytest.fixtures.scope2props['node'] = ()


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


if hasattr(_pytest.fixtures, 'scopename2class'):
    _pytest.fixtures.scopename2class.update({"global": Global, "node": Node})
