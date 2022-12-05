"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import asyncio
import os
import sys

import _pytest.terminal

from pytest_mproc.user_output import debug_print
from pytest_mproc.worker import WorkerSession

from pytest_mproc import user_output, Constants
from pytest_mproc.orchestration import Orchestrator, MainSession

import getpass
import shutil
import tempfile
from contextlib import contextmanager, suppress
from pathlib import Path
from traceback import format_exc
from typing import Callable, Optional, Union, Type


import pytest
import socket

from multiprocessing import cpu_count, JoinableQueue
from pytest_mproc.utils import is_degraded, BasicReporter


DEPLOY_TIMEOUT = None


# noinspection PyBroadException
def _get_ip_addr():
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return None


# noinspection SpellCheckingInspection
def parse_numprocesses(s: str) -> int:
    """
    A tiny bit of processing to get number of parallel processes to use (since "auto" can be used to represent
    # of cores on machine)
    :param s: text to process
    :return: number of parallel worker processes to use
    """
    is_worker = WorkerSession.singleton() is not None
    if is_worker:
        return 1  # override for worker that runs one worker per core
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


# noinspection PyProtectedMember,SpellCheckingInspection
def _add_option(group, name: str, dest: str, action: str,
                typ: Union[Type[int], Type[str], Type[bool], Callable[[str], int]], help_text: str) -> None:
    """
    internal add option function to allow us to keep track of pytest-specific options from command line
    """
    # noinspection PyProtectedMember
    if typ == bool:
        group._addoption(
            name,
            dest=dest,
            action=action,
            help=help_text,
        )
    else:
        group._addoption(
            name,
            dest=dest,
            action=action,
            type=typ,
            help=help_text,
        )
    Constants.set_ptmproc_args(name, typ)


# noinspection SpellCheckingInspection
@pytest.mark.tryfirst
def pytest_addoption(parser):
    """
    add options to given parser for this plugin
    """
    parser.addini('artifacts_dir', "Location to store generated artifacts from pytest_mproc, defaults to cwd")
    parser.addini('project_name', "Name of project being run, deafults to 'pytest_run' if not specified")
    group = parser.getgroup("pytest_mproc", "better distributed testing through multiprocessing")
    _add_option(
        group,
        "--cores",
        dest="mproc_numcores",
        action="store",
        typ=parse_numprocesses,
        help_text="you can use 'auto' here to set to the number of  CPU cores on host system",
    )
    _add_option(
        group,
        "--disable-mproc",
        dest="mproc_disabled",
        action="store",
        typ=bool,
        help_text="disable any parallel mproc testing, overriding all other mproc arguments",
    )
    _add_option(
        group,
        '--ptmproc-verbose',
        dest="mproc_verbose",
        action="store_true",
        typ=bool,
        help_text="output messages when connecting and executing tasks"
    )


def pytest_cmdline_main(config):
    """
    Called before "true" main routine.  This is to set up config values well ahead of time
    for things like pytest-cov that needs to know we are running distributed
    """
    mproc_disabled = config.getoption("mproc_disabled", default=False)
    mproc_num_cores = config.getoption("mproc_numcores", default=None)
    user_output.is_verbose = config.getoption("mproc_verbose", default=False)
    is_worker = WorkerSession.singleton() is not None
    is_orchestrator = MainSession.singleton() is not None
    if mproc_num_cores is None or is_degraded() or mproc_disabled:
        if is_orchestrator:
            raise pytest.UsageError(
                "Refusing to execute on distributed system without also specifying the number of cores to use"
                "(no '--cores <n>' on command line"
            )
        return
    if mproc_num_cores <= 0:
        raise pytest.UsageError(f"Invalid value for number of cores: {mproc_num_cores}")
    # tell xdist not to run
    config.option.dist = "no"
    val = config.getvalue
    if not val("collectonly"):
        # noinspection SpellCheckingInspection
        usepdb = config.getoption("usepdb")  # a core option
        if val("dist") != "no" and usepdb:
            raise pytest.UsageError(
                "--pdb is incompatible with distributing tests."
            )  # noqa: E501
    if is_worker:
        return mproc_pytest_cmdline_worker(config)
    elif is_orchestrator:
        return mproc_pytest_cmdline_orchestrator(config)
    else:
        # Running locally on local host only:
        return mproc_pytest_cmdline_main_local(config, mproc_num_cores)


# noinspection PyUnusedLocal
def mproc_pytest_cmdline_worker(config):
    pass


def mproc_pytest_cmdline_main_local(config, num_cores: int) -> None:
    reporter = BasicReporter()
    reporter.write(f"Running locally as main\n", green=True)
    orchestrator = Orchestrator.as_local()
    config.orchestrator = orchestrator
    report_q = JoinableQueue()
    orchestrator.start_session(session_id="LocalSession", cwd=Path(os.getcwd()), report_q=report_q, args=sys.argv[1:])
    for index in range(num_cores):
        worker_id = f"Worker-{index}"
        orchestrator.start_worker(session_id="LocalSession", worker_id=worker_id)


# noinspection PyUnusedLocal
def mproc_pytest_cmdline_orchestrator(config):
    pass


# noinspection SpellCheckingInspection
def pytest_sessionstart(session):
    with suppress(Exception):
        reporter = session.config.pluginmanager.getplugin('terminalreporter')
        if reporter:
            reporter._session = session


@pytest.mark.trylast
def pytest_configure(config):
    # if config.ptmproc_config.mode == ModeEnum.MODE_UNASSIGNED:
    #    return
    is_worker = WorkerSession.singleton() is not None
    if not is_worker and (config.getoption('mproc_num_cores', default=None) is None
       or config.getoption("mproc_disabled", default=False)):
        return
    # tell xdist not to run, (and BTW setting num_cores is enough to tell pycov we are distributed)
    config.option.dist = "no"


# noinspection PyProtectedMember,SpellCheckingInspection
def _process_fixtures(session, reporter, item):
    for name in item._fixtureinfo.argnames:
        try:
            fixturedef = item._fixtureinfo.name2fixturedefs.get(name, [None])[0]
            if not fixturedef or fixturedef.scope != 'node':
                continue
            if hasattr(fixturedef.func, "_pytest_group"):
                if hasattr(item._pyfuncitem.obj, "_pytest_group"):
                    group2 = item._pyfuncitem.obj._pytest_group
                    if group2 == fixturedef.func._pytest_group:
                        BasicReporter().write(
                            f"WARNING: '{item.nodeid}' specifies a group but also belongs " +
                            f"to fixture '{fixturedef.argname}'' which specifies the same group")
                    else:
                        raise pytest.UsageError(
                            session,
                            f"test {item.nodeid} belongs to group '{group2.name}' but also belongs " +
                            f"to fixture '{fixturedef.argname}' which specifies " +
                            f"group '{fixturedef.func._pytest_group.name}'.  A test cannot" +
                            "belong to two distinct groups")
                item._pyfuncitem._pytest_group = fixturedef.func._pytest_group
        except Exception as e:
            session.shouldfail = \
                f"Exception in fixture: {e.__class__.__name__} raised with msg '{str(e)}' {format_exc()}"
            reporter.write(f">>> Fixture ERROR: {format_exc()}\n", red=True)
            break


# noinspection PyProtectedMember,SpellCheckingInspection
def pytest_runtestloop(session):
    is_worker = WorkerSession.singleton() is not None
    is_orchestrator = MainSession.singleton() is not None
    debug_print(f"In run loop;  worker? {is_worker}; orchestrator? {is_orchestrator}")
    if session.config.option.collectonly:
        return
    if len(session.items) == 0:
        return
    if not is_worker and session.config.getoption("mproc_numcores", default=None) is None \
            or is_degraded() or getattr(session.config.option, "mproc_disabled"):
        # return of None indicates other hook impls will be executed to do the task at hand
        # aka, let the basic hook handle it from here, no distributed processing to be done
        return
    reporter: Optional[BasicReporter, _pytest.terminal.TerminalReporter] = \
        session.config.pluginmanager.getplugin('terminalreporter')
    if reporter:
        reporter.tests_count = len(session.items)
    if reporter is None:
        reporter = BasicReporter()
    if session.testsfailed and not session.config.option.continue_on_collection_errors:
        raise session.Interrupted("%d errors during collection!!" % session.testsfailed)
    for item in session.items:
        if session.shouldfail:
            break
        if hasattr(item, "_request"):
            _process_fixtures(session, reporter, item)
    if is_worker:
        with suppress(Exception):
            WorkerSession.singleton().test_loop(session)
    elif is_orchestrator:
        async def loop():
            try:
                await MainSession.singleton().run_loop(session, session.items)
            except asyncio.exceptions.CancelledError:
                pytest.exit("Tests canceled")
            except Exception as exc:
                reporter.write(f"{exc.__class__.__qualname__}: {exc}", red=True)
                # noinspection SpellCheckingInspection
                session.shouldfail = True
            finally:
                MainSession.singleton().shutdown()
        asyncio.run(loop())
    return True


@pytest.hookimpl(hookwrapper=True)
def pytest_collection_finish(session) -> None:
    config = session.config
    verbose = config.getoption("mproc_verbose", default=False)
    is_worker = WorkerSession.singleton() is not None
    if is_worker:
        config.option.verbose = -2
        with suppress(Exception):
            WorkerSession.singleton().pytest_collection_finish(session)
    yield
    config.option.verbose = verbose


# noinspection SpellCheckingInspection
def pytest_internalerror(excrepr):
    if WorkerSession.singleton():
        return WorkerSession.singleton().pytest_internalerror(excrepr)


# noinspection SpellCheckingInspection
@pytest.mark.tryfirst
def pytest_runtest_logreport(report):
    if WorkerSession.singleton():
        return WorkerSession.singleton().pytest_runtest_logreport(report)


# noinspection SpellCheckingInspection
@pytest.mark.tryfirst
def pytest_runtest_logstart(nodeid, location):
    if WorkerSession.singleton():
        return WorkerSession.singleton().pytest_runtest_logstart(nodeid=nodeid, location=location)


# noinspection SpellCheckingInspection
@pytest.mark.tryfirst
def pytest_runtest_logfinish(nodeid, location):
    if WorkerSession.singleton():
        return WorkerSession.singleton().pytest_runtest_logfinish(nodeid=nodeid, location=location)


# noinspection PyUnusedLocal,SpellCheckingInspection
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    verbose = config.getoption("mproc_verbose", default=False)
    is_worker = WorkerSession.singleton() is not None
    if is_worker:
        config.option.verbose = -2
        config.option.tbstyle = 'no'
        terminalreporter.reportchars = ""
        if user_output.is_verbose:
            terminalreporter.line(">>> This is a satellite processing node. "
                                  "Please see master node output for actual test summary <<<<<<<<<<<<",
                                  yellow=True)
    yield
    if is_worker:
        config.option.verbose = verbose


# noinspection PyUnusedLocal,SpellCheckingInspection
@pytest.hookimpl(hookwrapper=True)
def pytest_report_header(config, startdir):
    verbose = config.option.verbose
    if WorkerSession.singleton() is not None:
        config.option.verbose = -2
    yield
    config.option.verbose = verbose


# noinspection SpellCheckingInspection
@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_sessionstart(session) -> None:
    verbose = session.config.getoption("mproc_verbose", default=False)
    if WorkerSession.singleton() is not None:
        session.config.option.verbose = -2
    yield
    session.config.option.verbose = verbose


################
# Process-safe temp dir
###############


class TmpDirFactory:

    """
    tmpdir is not process/thread safe when used in a multiprocessing environment.  Failures on setup can
    occur (even if infrequently) under certain rae conditions.  This provides a safe mechanism for
    creating temporary directories utilizing s a global-scope fixture
    """

    def __init__(self):
        self._root_tmp_dir = tempfile.mkdtemp(prefix=f"pytest_mproc-{getpass.getuser()}-{os.getpid()}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with suppress(Exception):
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
                with suppress(Exception):
                    shutil.rmtree(tmpdir)


@pytest.fixture(scope='session')
def mp_tmp_dir_factory():
    """
    :return: a factory for creating unique tmp directories, unique across all Process's
    """
    with TmpDirFactory() as factory:
        yield factory


@pytest.fixture()
def mp_tmp_dir(mp_tmp_dir_factory: TmpDirFactory):
    # tmpdir is not thread safe and can fail on test setup when running on a highly loaded very parallelized system
    # so use this instead
    with mp_tmp_dir_factory.create_tmp_dir("PYTEST_MPROC_LAZY_CLEANUP" not in os.environ) as tmp_dir:
        yield tmp_dir
