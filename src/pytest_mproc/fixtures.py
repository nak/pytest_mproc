import os
import sys
import time
from multiprocessing import Queue
from multiprocessing.managers import BaseManager
from typing import Any

import _pytest

from pytest_mproc.config import MPManagerConfig, RoleEnum

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

        def __init__(self, value: Any):
            self._value = value

        def value(self):
            return self._value

    def __init__(self, host, port, is_master: bool):
        self._fixtures = {}
        self._is_master = is_master
        if not is_master:
            self.__class__.register("register_fixture")
            self.__class__.register("invoke_fixture")
            self.__class__.register("get_start_gate")
        else:
            self._start_gate = Queue()
            self.__class__.register("get_start_gate", lambda: self._start_gate)
            self.__class__.register("register_fixture", self._register_fixture)
            self.__class__.register("invoke_fixture", self._invoke)
        super().__init__((host, port), authkey=b'pass')
        self._on_hold = True

    def hold(self):
        """
        Hold up processing node or global level fixturedef (as worker thread) until master thread has processed
        all of those types of fixtures
        """
        if not self._on_hold:
            return
        start_gate = self.get_start_gate()
        v = start_gate.get()
        self._on_hold = False
        start_gate.put(v)
        if isinstance(v, Exception):
            raise v

    def release(self, val):
        """
        Called from master thread to signal worker threads all node/global fixtures are catalogued
        """
        self._on_hold = False
        self._start_gate.put(val)

    def _register_fixture(self, name, fixture):
        assert name not in self._fixtures, f"INTERNAL ERROR: {name} already in {self._fixtures}"
        self._fixtures[name] = self.Value(fixture)

    def _invoke(self, fixture_name):
        return self._fixtures[fixture_name]

    def get_fixturedef(self, fixturedef, param_index, ):
        name = fixturedef.argname
        if name in self.fixturedefs:
            fixturedef.cached_result = self.fixturedefs[name]
            return fixturedef.cached_result[0]
        my_cache_key = param_index
        try:
            self.hold()
            result = self.invoke_fixture(name).value()
        except _pytest.fixtures.TEST_OUTCOME:
            fixturedef.cached_result = (None, my_cache_key, sys.exc_info())
            self.fixturedefs[name] = (None, my_cache_key, sys.exc_info())
            raise
        if isinstance(result, Exception):
            fixturedef.cached_result = (None, my_cache_key, sys.exc_info())
            self.fixturedefs[name] = (None, my_cache_key, sys.exc_info())
            raise result
        fixturedef.cached_result = (result, my_cache_key, None)
        self.fixturedefs[name] = (result, my_cache_key, None)
        return result


class Node(_pytest.main.Session):

    class Manager(FixtureManager):
        fixturedefs = {}

        def __init__(self, config: MPManagerConfig):
            # scope to node: only run on localhost with random port
            as_master =config.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]
            super().__init__(config.node_mgr_host, config.node_mgr_port, as_master)
            if as_master:
                super().start()
            else:
                super().connect()


class Global(_pytest.main.Session):

    class Manager(FixtureManager):

        fixturedefs = {}

        def __init__(self, config: MPManagerConfig, force_as_client: bool = False):
            self._satellite_worker_count = 0
            self._test_q_exhausted = False
            if config.role != RoleEnum.MASTER or force_as_client:
                # client
                Global.Manager.register("get_test_queue")
                Global.Manager.register("get_results_queue")
                Global.Manager.register("register_satellite")
                Global.Manager.register("signal_satellite_done")
            else:
                # server:
                self._test_q = Queue()
                self._result_q = Queue()
                Global.Manager.register("get_test_queue", lambda: self._test_q)
                Global.Manager.register("get_results_queue", lambda: self._result_q)
                Global.Manager.register("register_satellite", self._register_satellite)
                Global.Manager.register("signal_satellite_done", self._signal_satellite_done)
                Global.Manager.register("signal_test_q_exhausted", self._signal_test_q_exhausted)
            super().__init__(config.global_mgr_host, config.global_mgr_port, config.role == RoleEnum.MASTER)
            if config.role == RoleEnum.MASTER and not force_as_client:
                super().start()
            else:
                tries_left = 60
                if config.role != RoleEnum.WORKER:
                    os.write(sys.stderr.fileno(), b"Waiting for server...")
                while tries_left:
                    try:
                        super().connect()
                        break
                    except ConnectionRefusedError as e:
                        if config.role != RoleEnum.WORKER:
                            os.write(sys.stderr.fileno(), b".")
                        tries_left -= 1
                        if tries_left == 0:
                            os.write(sys.stderr.fileno(), b"\n")
                            raise Exception(f"Failed to connect to server {config.global_mgr_host} port " +
                                            f"{config.global_mgr_port}: {str(e)}")
                        time.sleep(0.5)
                if config.role != RoleEnum.WORKER:
                    os.write(sys.stderr.fileno(), b"\nConnected\n")

        def _register_satellite(self, count: int):
            self._satellite_worker_count += count
            if self._test_q_exhausted:
                for _ in range(count):
                    self._test_q.put(None)

        def _signal_satellite_done(self, count: int):
            if count == 0:
                return
            self._satellite_worker_count -= count
            if self._satellite_worker_count <= 0:
                self._result_q.put(None)

        def _signal_test_q_exhausted(self):
            if not self._test_q_exhausted:
                for _ in range(self._satellite_worker_count):
                    self._test_q.put(None)
                self._test_q_exhausted = True

    def __init__(self, config):
        super().__init__(config)

    def setup(self):
        raise Exception("UNIMPL")

    def gethookproxy(self, fspath):
        raise Exception("UNIMPL")

    def isinitpath(self, path):
        return path in self.session._initialpaths

    def collect(self):
        raise Exception("UNIMPL")


_pytest.fixtures.scopename2class.update({"global": Global, "node": Node})
