import os
import signal
import sys
import time
from multiprocessing import Queue, Lock, JoinableQueue
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
            self.__class__.register("get_start_gate")
            self.__class__.register("wait_lock")
        else:
            self._start_gate = Queue()
            self._gate = Queue()
            self._wait_lock = Lock()
            self._wait_lock.acquire()
            self.__class__.register("get_start_gate", lambda: self._start_gate)
            self.__class__.register("wait_lock", lambda: self._wait_lock)
        super().__init__((host, port), authkey=b'pass')
        self._on_hold = True


class Node(_pytest.main.Session):
    pass


class Global(_pytest.main.Session):

    class Manager(FixtureManager):

        fixturedefs = {}

        def __init__(self, config: MPManagerConfig, force_as_client: bool = False):
            self._satellite_worker_count = 0
            self._mpconfig = config
            self._global_fixturedefs = {}
            if config.role != RoleEnum.MASTER or force_as_client:
                # client
                Global.Manager.register("register_satellite")
                Global.Manager.register("get_fixtures")
                Global.Manager.register("register_fixtures")
            else:
                # server:
                Global.Manager.register("register_satellite", self._register_satellite)
                Global.Manager.register("get_fixtures", self._get_fixtures)
                Global.Manager.register("register_fixtures", self._register_fixtures)
            super().__init__(config.global_mgr_host, config.global_mgr_port, config.role == RoleEnum.MASTER)
            if config.role == RoleEnum.MASTER and not force_as_client:
                super().start()
            else:
                def timeout(*args):
                    raise TimeoutError()

                signal.signal(signal.SIGALRM, timeout)
                tries_remaining = 60
                if config.role != RoleEnum.WORKER:
                    os.write(sys.stderr.fileno(), b"Waiting for server...\n")
                while tries_remaining:
                    try:
                        signal.alarm(1)
                        super().connect()
                        signal.alarm(0)
                        break
                    except (ConnectionResetError, ConnectionError, ConnectionAbortedError, ConnectionRefusedError) as e:
                        if config.role != RoleEnum.WORKER:
                            os.write(sys.stderr.fileno(), b".")
                        tries_remaining -= 1
                        if tries_remaining == 0:
                            os.write(sys.stderr.fileno(), b"\n")
                            raise Exception(f"Failed to connect to server {config.global_mgr_host} port " +
                                            f"{config.global_mgr_port}: {str(e)}")
                        time.sleep(0.5)
                    except TimeoutError:
                        tries_remaining -= 1
                        time.sleep(0.5)

                if config.role != RoleEnum.WORKER:
                    os.write(sys.stderr.fileno(), b"\nConnected\n")

        def _register_satellite(self, count: int):
            self._satellite_worker_count += count

        def _get_fixtures(self):
            v =self._gate.get()
            self._gate.put(v)
            return self._global_fixturedefs

        def _register_fixtures(self, fixturedefs):
            self._gate.put(True)
            self._global_fixturedefs = fixturedefs

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
