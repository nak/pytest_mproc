"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import getpass
import inspect
import os
import shutil
import socket
import sys
import tempfile
import time
import traceback
from contextlib import closing, contextmanager
from multiprocessing.managers import BaseManager
from pathlib import Path
from typing import Any

import _pytest
_pytest.fixtures.scopes.insert(0, "node")
_pytest.fixtures.scopes.insert(0, "global")
_pytest.fixtures.scopenum_function = _pytest.fixtures.scopes.index("function")
_pytest.fixtures.scope2props['global'] = ()
_pytest.fixtures.scope2props['node'] = ()

import pytest

from multiprocessing import cpu_count, Queue
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.utils import is_degraded


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


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
        if not self._on_hold:
            return
        start_gate = self.get_start_gate()
        v = start_gate.get()
        self._on_hold = False
        start_gate.put(v)
        if isinstance(v, Exception):
            raise v

    def release(self, val):
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
        PORT = find_free_port()
        fixturedefs = {}

        def __init__(self, config):
            # scope to node: only run on localhost with random port
            is_master = not hasattr(config.option, "mproc_worker")
            super().__init__('127.0.0.1', self.PORT, is_master)
            if is_master:
                super().start()
            else:
                super().connect()


class Global(_pytest.main.Session):

    class Manager(FixtureManager):

        fixturedefs = {}

        DEFAULT_PORT = find_free_port()

        def __init__(self, config, is_master_flag: bool = True):
            self._satellite_worker_count = 0
            self._test_q_exhausted = False
            self.is_master = is_master_flag and getattr(config.option, "mproc_worker", None) is None and config.option.is_master
            if not self.is_master:
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
            super().__init__(config.option.mproc_server_host, config.option.mproc_server_port or self.DEFAULT_PORT,
                             self.is_master)
            if self.is_master:
                super().start()
            else:
                tries_left = 60
                if not config.option.is_master:
                    os.write(sys.stderr.fileno(), b"Waiting for server...")
                while tries_left:
                    try:
                        super().connect()
                        break
                    except ConnectionRefusedError as e:
                        if not config.option.is_master:
                            os.write(sys.stderr.fileno(), b".")
                        tries_left -= 1
                        if tries_left == 0:
                            os.write(sys.stderr.fileno(), b"\n")
                            raise Exception(f"Failed to connect to server {config.option.mproc_server_host} port " +
                                            f"{config.option.mproc_server_port or self.DEFAULT_PORT}: {str(e)}")
                        time.sleep(0.5)
                if not config.option.is_master:
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


_getscopeitem_orig = _pytest.fixtures.FixtureRequest._getscopeitem


def _getscopeitem_redirect(self, scope):
    if scope == "global" or scope == "node":
        return self._pyfuncitem.getparent(_pytest.main.Session)
    else:
        return _getscopeitem_orig(self, scope)


_pytest.fixtures.FixtureRequest._getscopeitem = _getscopeitem_redirect
_pytest.fixtures.scopename2class.update({"global": Global, "node": Node})

def parse_numprocesses(s):
    """
    A little bit of processing to get number of parallel processes to use (since "auto" can be used to represent
    # of cores on machine)
    :param s: text to process
    :return: number of parallel worker processes to use
    """
    try:
        if s.startswith("auto"):
            if '*' in s:
                multiplication_factor = int(s.rsplit('*', 1)[-1])
            elif s == "auto":
                multiplication_factor = 1
            else:
                raise Exception("Error: --cores argument must be an integer value or auto or auto*<int factor>")
            return cpu_count() * multiplication_factor
        else:
            return int(s)
    except ValueError:
        raise Exception("Error: --cores argument must be an integer value or \"auto\" or \"auto*<int factor>\"")


@pytest.mark.tryfirst
def pytest_addoption(parser):
    """
    add options to given parser for this plugin
    """
    group = parser.getgroup("pytest_mproc", "better distributed testing through multiprocessing")
    group._addoption(
        "--cores",
        dest="mproc_numcores",
        metavar="mproc_numcores",
        action="store",
        type=parse_numprocesses,
        help="you can use 'auto' here to set to the number of  CPU cores on host system",
    )
    group._addoption(
            "--disable-mproc",
            dest="mproc_disabled",
            metavar="mproc_disabled",
            action="store",
            type=bool,
            help="disable any parallel mproc testing, overriding all other mproc arguments",
        )
    group._addoption(
        "--as-server",
        dest="mproc_server_port",
        metavar="mproc_server_port",
        action="store",
        type=int,
        help="port on which you wish to run server (for multi-host runs only)"
    )
    group._addoption(
        "--as-client",
        dest="mproc_client_connect",
        metavar="mproc_client_connect",
        action="store",
        type=str,
        help="host:port specification of master node to connect to as client"
    )


@pytest.mark.tryfirst
def pytest_cmdline_main(config):
    """
    Called before "true" main routine.  This is to set up config values well ahead of time
    for things like pytest-cov that needs to know we are running distributed

    Mostly taken from other implementations (such as xdist)
    """
    worker = getattr(config.option, "mproc_worker", None)
    # validation
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        config.option.is_master = True
        config.option.mproc_server_host = '127.0.0.1'
        config.option.mproc_server_port = find_free_port()
        config.option.mproc_manager = Global.Manager(config)
        config.option.mproc_node_manager = Node.Manager(config)
        print(">>>>> no number of cores provided or running in environment unsupportive of parallelized testing, "
              "not running multiprocessing <<<<<")
        return
    config.option.numprocesses = config.option.mproc_numcores  # this is what pycov uses to determine we are distributed
    mproc_server_port = getattr(config.option, 'mproc_server_port', None)
    mproc_client_connect = getattr(config.option, "mproc_client_connect", None)
    if mproc_client_connect and mproc_server_port:
        raise pytest.UsageError("Cannot specify both -as-master and --as-client at same time")
    elif mproc_server_port:
        config.option.is_master = True
        config.option.mproc_server_host = socket.gethostbyname(socket.gethostname())
        if not worker:
            print(f"Running as master: {config.option.mproc_server_host}:{mproc_server_port}")
        if config.option.numprocesses < 0:
            raise pytest.UsageError("Number of cores must be greater than or equal to zero when running as a master")
    elif mproc_client_connect:
        config.option.is_master = False
        try:
            host, port = mproc_client_connect.rsplit(':', maxsplit=1)
        except Exception:
            raise pytest.UsageError("--as-client must be specified in form '<host>:<port>' of the master node")
        try:
            port = int(port)
        except ValueError:
            raise pytest.UsageError("When specifying connection as client, port must be an integer value")
        config.option.mproc_server_host = host
        config.option.mproc_server_port = port
        if config.option.numprocesses < 1:
            raise pytest.UsageError("Number of cores must be 1 or more when running as client")
    else:
        config.option.is_master = True  # we're the only node running
        config.option.mproc_server_host = '127.0.0.1'
        config.option.mproc_server_port = None
        if config.option.numprocesses < 1:
            raise pytest.UsageError("Number of cores must be 1 or more when running on a single node")
    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
    config.option.dist = "no"
    val = config.getvalue
    if not val("collectonly"):
        usepdb = config.getoption("usepdb")  # a core option
        if val("dist") != "no":
            if usepdb:
                raise pytest.UsageError(
                    "--pdb is incompatible with distributing tests."
                )  # noqa: E501
    config.coordinator = None
    if not worker:
        # in main thread,
        # instantiate coordinator here and start to kick off processing on workers early, so they can
        # process config info in parallel to this thread
        config.coordinator = Coordinator(config.option.mproc_numcores,
                                         is_master=config.option.is_master)
    config.option.mproc_manager = Global.Manager(config)
    if not worker:
        config.coordinator.start(config)
    config.option.mproc_node_manager = Node.Manager(config)


@pytest.mark.trylast
def pytest_configure(config):
    worker = getattr(config.option, "mproc_worker", None)
    server_host = getattr(config.option, "mproc_server_host", None)
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        return  # return of None indicates other hook impls will be executed to do the task at hand
    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
    config.option.dist = "no"


@pytest.mark.tryfirst
def pytest_fixture_setup(fixturedef, request):
    if fixturedef.scope == 'node':
        param_index = request.param_index if not hasattr(request, "param") else request.param
        return request.config.option.mproc_node_manager.get_fixturedef(fixturedef, param_index)
    elif fixturedef.scope == 'global':
        return request.config.option.mproc_manager.get_fixturedef(fixturedef, request)
    return _pytest.fixtures.pytest_fixture_setup(fixturedef, request)


def process_fixturedef(name: str, item, session, global_fixtures, node_fixtures):
    try:
        request = item._request
        fixturedef = item._fixtureinfo.name2fixturedefs.get(name, None)
        generated = []
        if fixturedef and fixturedef[0].scope == 'global' and name not in global_fixtures:
            if session.config.option.is_master:
                global_fixtures[name] = _pytest.fixtures.resolve_fixture_function(fixturedef[0], request)
                val = global_fixtures[name](*fixturedef[0].argnames)
                if inspect.isgenerator(val):
                    generated.append(val)
                    val = next(val)
                try:
                    session.config.option.mproc_manager.register_fixture(name, val)
                except TypeError as e:
                    request.config.option.mproc_manager.register_fixture(name, e)
                    session.should_fail = f">>> Cannot pickle global fixture object: {e}"
                except Exception as e:
                    request.config.option.mproc_manager.register_fixture(name, e)
                    session.should_fail = f">>> Exception in multiprocess communication: {e}"
        if fixturedef and fixturedef[0].scope == 'node' and name not in node_fixtures:
            node_fixtures[name] = _pytest.fixtures.resolve_fixture_function(fixturedef[0], request)
            val = node_fixtures[name](*fixturedef[0].argnames)
            if inspect.isgenerator(val):
                generated.append(val)
                val = next(val)
            try:
                session.config.option.mproc_node_manager.register_fixture(name, val)
            except TypeError as e:
                request.config.option.mproc_node_manager.register_fixture(name, e)
                session.should_fail = f">>> Cannot pickle global fixture object: {e}"
            except Exception as e:
                request.config.option.mproc_node_manager.register_fixture(name, e)
                session.should_fail = f">>> Exception in multiprocess communication: {e}"

        if fixturedef:
            for argname in fixturedef[0].argnames:
                generated += process_fixturedef(argname, item, session, global_fixtures, node_fixtures)
        return generated
    except Exception as e:
        print(traceback.format_exc())
        session.shouldfail = str(e)
        return generated


@pytest.mark.tryfirst
def pytest_runtestloop(session):
    global_fixtures = {}
    node_fixtures = {}
    if session.testsfailed and not session.config.option.continue_on_collection_errors:
        raise session.Interrupted("%d errors during collection" % session.testsfailed)

    if session.config.option.collectonly:
        return  # should never really get here, but for consistency

    worker = getattr(session.config.option, "mproc_worker", None)
    session.config.option.generated = []
    if worker is None:
        for item in session.items:
            if session.shouldfail:
                break
            if hasattr(item, "_request"):

                for name in item._fixtureinfo.argnames:
                    if name not in global_fixtures and name not in node_fixtures:
                        session.config.option.generated += process_fixturedef(name, item, session,
                                                                              global_fixtures, node_fixtures)
                    if session.shouldfail:
                        break
    if session.shouldfail:
        print(f">>> Fixture ERROR: {session.shouldfail}")
        if session.config.option.is_master:
            session.config.option.mproc_manager.release(session.Failed(session.shouldfail))
        if worker is None:
            session.config.option.mproc_node_manager.release(session.Failed(session.shouldfail))
        if not worker and hasattr(session.config, 'coordinator'):
            session.config.coordinator.kill()
        raise session.Failed(session.shouldfail)
    # signals any workers waiting on global fixture(s) to continue
    if worker is None and session.config.option.is_master:
        session.config.option.mproc_manager.release(True)
    if worker is None:
        session.config.option.mproc_node_manager.release(True)
    if getattr(session.config.option, "mproc_numcores", None) is None or is_degraded() or getattr(session.config.option, "mproc_disabled"):
        # return of None indicates other hook impls will be executed to do the task at hand
        # aka, let the basic hook handle it from here, no distributed processing to be done
        return

    if not session.config.getvalue("collectonly") and worker is None:
        # main coordinator loop:
        with session.config.coordinator as coordinator:
            try:
                coordinator.set_items(session.items)
                coordinator.run(session)
            except Exception as e:
                print(f"\n>>> ERROR in run loop;  unexpected Exception\n {str(e)}\n")
                return False
    else:
        worker.test_loop(session)

    return True


def pytest_sessionfinish(session):
    worker = getattr(session.config.option, "mproc_worker", None)
    if worker is None and hasattr(session.config.option, "mproc_manager"):
        if session.config.option.mproc_manager.is_master:
            try:
                session.config.option.mproc_manager.shutdown()
            except Exception as e:
                print(">>> INTERNAL Error shutting down mproc manager")
    if worker is None and hasattr(session.config.option, "mproc_node_manager"):
        try:
            session.config.option.mproc_node_manager.shutdown()
        except Exception as e:
            print(">>> INTERNAL Error shutting down mproc manager")
    generated = getattr(session.config.option, "generated", [])
    for item in generated:
        try:
            next(item)
            raise Exception(f"Fixture generator did not stop {item}")
        except StopIteration:
            pass

################
# Process-safe temp dir
###############


class TmpDirFactory:

    """
    tmpdir is not process/thread safe when used in a multiprocessing environment.  Failures on setup can
    occur (even if infrequently) under certain rae conditoins.  This provides a safe mechanism for
    creating temporary directories utilizng s a global-scope fixture
    """
    _root_tmp_dir = tempfile.mkdtemp(prefix=f"pytest_mproc-{getpass.getuser()}-{os.getpid()}")

    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self._root_tmp_dir)

    @contextmanager
    def create_tmp_dir(self, cleanup_immediately: bool = True):
        """
        :param cleanup_immediately: if True, rm the directory and all contents when associated fixture is no longer
           in use, otherwise wait until end of test session when everything is cleaned up
        :return: newly create temp directory unique across all Process's
        """
        tmpdir = tempfile.mkdtemp(dir=self._root_tmp_dir)
        try:
            yield Path(tmpdir)
        finally:
            if cleanup_immediately:
                shutil.rmtree(tmpdir)


@pytest.fixture(scope='node')
def mp_tmpdir_factory():
    """
    :return: a factory for creating unique tmp directories, unique across all Process's
    """
    with TmpDirFactory() as factory:
        yield factory


@pytest.fixture()
def mp_tmp_dir(mp_tmpdir_factory: TmpDirFactory):
    # tmpdir is not thread safe and can fail on test setup when running on a highly loaded very parallelized system
    # so use this instead
    with mp_tmpdir_factory.create_tmp_dir("PYTEST_MPROC_LAZY_CLEANUP" not in os.environ) as tmp_dir:
        yield tmp_dir
