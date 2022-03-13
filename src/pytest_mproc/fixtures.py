import os
import queue
import sys
from enum import Enum
from multiprocessing import current_process
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Tuple, Optional, Union

import _pytest
from _pytest import nodes
from _pytest.compat import assert_never

from pytest_mproc import find_free_port

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

    class Value:

        def __init__(self, val: Any):
            self._val = val

        def value(self) -> Any:
            return self._val

    def __init__(self, addr: Tuple[str, int], auth_key: Optional[bytes] = None):
        print(f"[{os.getpid()}]>>>>>>>>>>>>> SERVER USING AUTH KEY {auth_key or current_process().authkey}")
        super().__init__(address=addr, authkey=auth_key or current_process().authkey)

    # noinspection PyAttributeOutsideInit
    def start(self, *args, **kwargs):
        # server:
        self._fixtures: Dict[str, Any] = {}
        self._fixture_q = queue.Queue()
        self.__class__.register("get_fixture", self.get_fixture)
        self.__class__.register("put_fixture", self.put_fixture)
        try:
            super().start(*args, **kwargs)
        except OSError:
            print(f"!!!!!!!!!!!!!!!!! ERROR STARTING {self}")
            raise

    def connect(self):
        self.__class__.register("get_fixture_")
        self.__class__.register("put_fixture")
        super().connect()

    def put_fixture(self, name: str, value: Any) -> None:
        self._fixtures[name] = value
        self._fixture_q.put((name, value))

    def get_fixture(self, name: str) -> Any:
        result = self._fixtures.get(name)
        while result is None:
            name, value = self._fixture_q.get()
            self._fixtures[name] = value
            result = self._fixtures.get(name)

        return self.Value(result)


class Node(_pytest.main.Session):

    class Manager(FixtureManager):

        def __init__(self, as_main: bool, port: int, name: str = "Node.Manager", connection_timeout:int = 30):
            if not as_main:
                os.write(sys.stderr.fileno(), f"Waiting for server...[{name}]\n".encode('utf-8'))
            super().__init__(("127.0.0.1", port))
            if not as_main:
                os.write(sys.stderr.fileno(), f"Connected [{name}]\n".encode('utf-8'))

        PORT = 7038
        _singleton = None

        @classmethod
        def singleton(cls) -> "Node.Manager":
            if cls._singleton is None:
                try:
                    cls._singleton = cls(as_main=True, port=cls.PORT)
                    cls._singleton.start()
                    cls._singleton._is_serving = True
                except (OSError, EOFError):
                    cls._singleton = cls(as_main=False, port=cls.PORT, connection_timeout=10)
                    cls._singleton.connect()
                    cls._singleton._is_serving = False
            return cls._singleton

        @classmethod
        def shutdown(cls) -> None:
            if cls._singleton is not None and cls._singleton._is_serving:
                cls._singleton.shutdown()
                cls._singleton = None


class Global(_pytest.main.Session):

    def __init__(self, config):
        super().__init__(config)


if hasattr(_pytest.fixtures, 'scopename2class'):
    _pytest.fixtures.scopename2class.update({"global": Global, "node": Node})
