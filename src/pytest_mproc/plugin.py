"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import functools
from multiprocessing.managers import BaseManager

import _pytest
_pytest.fixtures.scopes.insert(0, "global")
_pytest.fixtures.scopenum_function = _pytest.fixtures.scopes.index("function")
import contextlib
import pytest

from multiprocessing import cpu_count
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.utils import is_degraded


class Global(_pytest.main.Session):

    class Manager(BaseManager):

        ADDRESS = ('127.0.0.1', 33981)

        def __init__(self, config):
            super().__init__(self.ADDRESS, authkey=b'pass')
            self._fixtures = {}
            if hasattr(config.option, "mproc_worker"):
                # client
                Global.Manager.register("invoke_fixture")
                super().connect()
            else:
                Global.Manager.register("invoke_fixture", self._invoke)
                Global.Manager.register("register_fixture", self._register_fixture)
                super().start()

        def _register_fixture(self, name, fixture):
            self._fixtures[name] = fixture

        def _invoke(self, fixture_name):
            return self._fixtures[fixture_name]

    _manager = None

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
        if False and hasattr(self.config.option, "mproc_numcores"):
            manager = Global.Manager(self.config, self._pyfuncitem, self._fixturedef.argname)

            @functools.wraps(self._pyfuncitem)
            def wrap(*args, **kargs):
                return manager.invoke_fixture(self._fixturedef.argname, *args, **kargs)

            self._pyfuncitem = wrap
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


@pytest.mark.tryfirst
def pytest_runtestloop(session):
    if session.testsfailed and not session.config.option.continue_on_collection_errors:
        raise session.Interrupted("%d errors during collection" % session.testsfailed)

    if session.config.option.collectonly:
        return  # should never really get here, but for consistency

    worker = getattr(session.config.option, "mproc_worker", None)
    if worker is None and hasattr(session.config.option, "mproc_numcores"):
        global_fixtures = {}
        for item in session.items:
            if hasattr(item, "_request"):
                request = item._request
                for name in item._fixtureinfo.argnames:
                    fixturedef = item._fixtureinfo.name2fixturedefs.get(name, None)
                    if not fixturedef or fixturedef[0].scope != 'global' or name in global_fixtures:
                        continue
                    global_fixtures[name] = _pytest.fixtures.resolve_fixture_function(fixturedef[0], request)
                    session.config.option.mproc_manager.register_fixture(name, global_fixtures[name](fixturedef[0].argnames))
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
        session.config.option.mproc_manager.shutdown()
