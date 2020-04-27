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
import traceback
from contextlib import closing, contextmanager
from multiprocessing.managers import BaseManager
from pathlib import Path
from typing import Any

import _pytest
_pytest.fixtures.scopes.insert(0, "global")
_pytest.fixtures.scopenum_function = _pytest.fixtures.scopes.index("function")
import pytest

from multiprocessing import cpu_count, Queue
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.utils import is_degraded


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class Global(_pytest.main.Session):

    class Manager(BaseManager):

        ADDRESS = ('127.0.0.1', find_free_port())
        _start_gate = Queue()

        class Value:

            def __init__(self, value: Any):
                self._value = value

            def value(self):
                return self._value

        def __init__(self, config):
            super().__init__(self.ADDRESS, authkey=b'pass')
            self._fixtures = {}
            self._on_hold = True
            if hasattr(config.option, "mproc_worker"):
                # client
                Global.Manager.register("invoke_fixture")
                Global.Manager.register("sem")
                super().connect()
            else:
                # server:
                Global.Manager.register("invoke_fixture", self._invoke)
                Global.Manager.register("register_fixture", self._register_fixture)
                super().start()

        def hold(self):
            if not self._on_hold:
                return
            v = self._start_gate.get()
            self._on_hold = False
            self._start_gate.put(v)
            if isinstance(v, Exception):
                raise v

        def release(self, val):
            self._on_hold = False
            self._start_gate.put(val)

        def _register_fixture(self, name, fixture):
            self._fixtures[name] = self.Value(fixture)

        def _invoke(self, fixture_name):
            return self._fixtures[fixture_name]

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
    if scope == "global":
        return self._pyfuncitem.getparent(_pytest.main.Session)
    else:
        return _getscopeitem_orig(self, scope)


_pytest.fixtures.FixtureRequest._getscopeitem = _getscopeitem_redirect
_pytest.fixtures.scopename2class.update({"global": Global})
_pytest.fixtures.scope2props['global'] = ()


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


@pytest.mark.tryfirst
def pytest_cmdline_main(config):
    """
    Called before "true" main routine.  This is to set up config values well ahead of time
    for things like pytest-cov that needs to know we are running distributed

    Mostly taken from other implementations (such as xdist)
    """
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        print(">>>>> no number of cores provided or running in environment unsupportive of parallelized testing, "
              "not running multiprocessing <<<<<")
        return
    config.option.numprocesses = config.option.mproc_numcores  # this is what pycov uses to determine we are distributed
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


@pytest.mark.trylast
def pytest_configure(config):
    config.option.mproc_manager = Global.Manager(config)
    worker = getattr(config.option, "mproc_worker", None)
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        return  # return of None indicates other hook impls will be executed to do the task at hand
    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
    config.option.dist = "no"
    if not worker:
        # in main thread,
        # instantiate coordinator here and start to kick off processing on workers early, so they can
        # process config info in parallel to this thread
        config.coordinator = Coordinator(config.option.mproc_numcores)
        config.coordinator.start()


global_fixturedefs = {}


@pytest.mark.tryfirst
def pytest_fixture_setup(fixturedef, request):
    if fixturedef.scope == 'global':
        name = fixturedef.argname
        if name in global_fixturedefs:
            fixturedef.cached_result = global_fixturedefs[name]
            return fixturedef.cached_result[0]
        my_cache_key = fixturedef.cache_key(request)
        try:
            request.config.option.mproc_manager.hold()
            result = request.config.option.mproc_manager.invoke_fixture(name).value()
        except _pytest.fixtures.TEST_OUTCOME:
            fixturedef.cached_result = (None, my_cache_key, sys.exc_info())
            global_fixturedefs[name] = (None, my_cache_key, sys.exc_info())
            raise
        if isinstance(result, Exception):
            fixturedef.cached_result = (None, my_cache_key, sys.exc_info())
            global_fixturedefs[name] = (None, my_cache_key, sys.exc_info())
            raise result
        fixturedef.cached_result = (result, my_cache_key, None)
        global_fixturedefs[name] = (result, my_cache_key, None)
        return result
    return _pytest.fixtures.pytest_fixture_setup(fixturedef, request)


def process_fixturedef(name: str, item, session, global_fixtures):
    try:
        request = item._request
        fixturedef = item._fixtureinfo.name2fixturedefs.get(name, None)
        generated = []
        if fixturedef and fixturedef[0].scope == 'global' and name not in global_fixtures:
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

        if fixturedef:
            for argname in fixturedef[0].argnames:
                generated += process_fixturedef(argname, item, session, global_fixtures)
        return generated
    except Exception as e:
        print(traceback.format_exc())
        session.shouldfail = str(e)
        return generated


@pytest.mark.tryfirst
def pytest_runtestloop(session):
    global_fixtures = {}
    if session.testsfailed and not session.config.option.continue_on_collection_errors:
        raise session.Interrupted("%d errors during collection" % session.testsfailed)

    if session.config.option.collectonly:
        return  # should never really get here, but for consistency

    worker = getattr(session.config.option, "mproc_worker", None)
    generated = []
    if worker is None and hasattr(session.config.option, "mproc_numcores"):

        for item in session.items:
            if session.shouldfail:
                break
            if hasattr(item, "_request"):

                for name in item._fixtureinfo.argnames:
                    if name not in global_fixtures:
                        generated += process_fixturedef(name, item, session, global_fixtures)
                    if session.shouldfail:
                        break
    if session.shouldfail:
        print(f">>> Fixture ERROR: {session.shouldfail}")
        session.config.option.mproc_manager.release(session.Failed(session.shouldfail))
        if not worker and hasattr(session.config, 'coordinator'):
            session.config.coordinator.kill()
        raise session.Failed(session.shouldfail)
    session.config.option.mproc_manager.release(True)
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
    for item in generated:
        try:
            next(item)
            raise Exception("Generator did not stop")
        except StopIteration:
            pass
    return True


def pytest_sessionfinish(session):
    worker = getattr(session.config.option, "mproc_worker", None)
    if worker is None and hasattr(session.config.option, "mproc_manager"):
        try:
            session.config.option.mproc_manager.shutdown()
        except Exception as e:
            print(">>> Error shutting down mproc manager")


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
        print(">>>>>>> REMOVING TEMPDIR ROOT")
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


@pytest.fixture(scope='global')
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
