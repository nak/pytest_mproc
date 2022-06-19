"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import asyncio
import os
import sys

import _pytest.terminal

from pytest_mproc import worker, user_output, Constants
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.http import HTTPSession
from pytest_mproc.main import Orchestrator
from pytest_mproc.orchestration import OrchestrationManager
from pytest_mproc.user_output import always_print

import getpass
import shutil
import tempfile
from contextlib import contextmanager, suppress
from pathlib import Path
from traceback import format_exc
from typing import Callable, Optional, Iterable, Union, Type

from pytest_mproc.ptmproc_data import (
    PytestMprocConfig,
    RemoteWorkerConfig,
    ProjectConfig,
    PytestMprocRuntime,
)


import pytest
from pytest_mproc import find_free_port, fixtures

import socket

from multiprocessing import cpu_count
from pytest_mproc.utils import is_degraded, BasicReporter


DEPLOY_TIMEOUT = None


# noinspection PyBroadException
def _get_ip_addr():
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        print(">>> Cannot get ip address of host.  Return 127.0.0.1 (localhost)")
        return "127.0.0.1"


def parse_numprocesses(s: str) -> int:
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


# noinspection PyProtectedMember
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


@pytest.mark.tryfirst
def pytest_addoption(parser):
    """
    add options to given parser for this plugin
    """
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
        "--max-simultaneous-connections",
        dest="mproc_max_simultaneous_connections",
        action="store",
        typ=int,
        help_text="max # of connections allowed at one time to main process, to prevent deadlock from overload"
    )
    _add_option(
        group,
        "--as-main",
        dest="mproc_server_uri",
        action="store",
        typ=str,
        help_text="port on which you wish to run server (for multi-host runs only)"
    )
    _add_option(
        group,
        "--remote-worker",
        dest="mproc_remote_clients",
        action="append",
        typ=str,
        help_text="repeatable option to add remote client for execution. string is of "
        "form '<host>[:port][;<key>=<value>]*, "
        "where key, value pairs are translated into command line argument to the host in form '--<key> <value>'"
    )
    if '--as-main' in sys.argv or '--remote-worker' in sys.argv:
        _add_option(
            group,
            "--project-structure",
            dest="project_structure_path",
            action="store",
            typ=str,
            help_text="path to project structure (if server)"
        )
        _add_option(
            group,
            "--remote-sys-executable",
            dest="mproc_remote_sys_executable",
            typ=str,
            help_text="common path on remote hosts to python executable to use",
            action="store"
        )
    else:
        _add_option(
            group,
            "--as-worker",
            dest="mproc_client_connect",
            action="store",
            typ=str,
            help_text="host:port specification of master node to connect to as client"
        )
    _add_option(
        group,
        "--connection-timeout",
        dest="mproc_connection_timeout",
        action="store",
        typ=int,
        help_text="wait this many seconds on connection of client before timing out"
    )
    _add_option(
        group,
        '--ptmproc-verbose',
        dest="mproc_verbose",
        action="store_true",
        typ=bool,
        help_text="output messages when connecting and executing tasks"
    )


cmdline_main_called = False


def pytest_cmdline_main(config):
    """
    Called before "true" main routine.  This is to set up config values well ahead of time
    for things like pytest-cov that needs to know we are running distributed

    Mostly taken from other implementations (such as xdist)
    """
    global cmdline_main_called
    if cmdline_main_called:
        return
    cmdline_main_called = True
    mproc_max_connections = getattr(config.option, "mproc_max_simultaneous_connections", 24)
    mproc_connection_timeout = getattr(config.option, "mproc_connection_timeout", None)
    mproc_disabled = getattr(config.option, "mproc_disabled")
    mproc_num_cores = getattr(config.option, "mproc_numcores", None)
    user_output.verbose = config.option.mproc_verbose
    if "PYTEST_WORKER" in os.environ:
        config.option.ptmproc_config = PytestMprocConfig(num_cores=1)
        config.ptmproc_runtime = None
        worker.pytest_cmdline_main(config)
        assert config.option.worker
        return
    else:
        config.option.worker = None
        if config.option.mproc_remote_clients and not config.option.mproc_server_uri:
            config.option.mproc_server_uri = "delegated://"
    if not config.option.mproc_remote_clients and not config.option.mproc_server_uri:
        # pure local so start Global Manager
        from pytest_mproc.fixtures import Global
        port = find_free_port()
        Global.Manager.singleton(('localhost', port), as_client=False)
        os.environ['PTMPROC_GM_URI'] = f"localhost:{port}"
        port = find_free_port()
        config.option.mproc_server_uri = f"localhost:{port}"
    if not hasattr(config, "ptmproc_runtime"):
        config.ptmproc_runtime = None
    config.option.ptmproc_config = PytestMprocConfig()
    if mproc_connection_timeout:
        config.option.ptmproc_config.connection_timeout = mproc_connection_timeout
    reporter = BasicReporter()
    config.option.ptmproc_config.num_cores = mproc_num_cores
    config.option.ptmproc_config.max_simultaneous_connections = mproc_max_connections or 10
    # this is what pycov uses to determine we are distributed:
    config.option.numprocesses = config.option.ptmproc_config.num_cores
    if config.option.numprocesses and config.option.numprocesses < 0:
        raise pytest.UsageError("Number of cores must be greater than or equal to zero when running as a master")
    if config.option.ptmproc_config.max_simultaneous_connections and \
            config.option.ptmproc_config.max_simultaneous_connections <= 0:
        raise pytest.UsageError("max simultaneous connections must be greater than 0; preferably greater than 9")
    # validation
    if config.option.ptmproc_config.num_cores is None or is_degraded() or mproc_disabled:
        if '--as-main' in sys.argv or '--as-worker' in sys.argv:
            raise pytest.UsageError(
                "Refusing to execute on distributed system without also specifying the number of cores to use")
        reporter.write(
            ">>> no number of cores provided or running in environment unsupportive of parallelized testing, "
            "not running multiprocessing <<<<<\n", yellow=True)
        return
    if getattr(config.option, "mproc_client_connect", None):
        mproc_pytest_cmdline_coordinator(config)
    else:
        mproc_pytest_cmdline_main(config, reporter=reporter)


def mproc_pytest_cmdline_coordinator(config):
    config.option.no_summary = True
    config.option.no_header = True
    config.option.ptmproc_config = PytestMprocConfig(
        num_cores=config.option.ptmproc_config.num_cores,
        server_uri=None,
        client_connect=getattr(config.option, "mproc_client_connect", None)
    )
    if config.option.ptmproc_config.num_cores < 1:
        raise pytest.UsageError("Number of cores must be 1 or more when running as client")
    if config.option.ptmproc_config.client_connect and not config.option.worker:
        assert 'PTMPROC_WORKER' not in os.environ
        uri = config.option.ptmproc_config.client_connect
        always_print(f"Running coordinator connecting to {uri} [{os.getpid()}]")
        config.option.ptmproc_config.server_uri = uri
        if not config.option.collectonly:
            coordinator = Coordinator()
            config.ptmproc_runtime = PytestMprocRuntime(mproc_main=None,
                                                        coordinator=coordinator)
            mgr = OrchestrationManager.create(uri=uri, as_client=True, project_name=None)
            mgr.register_coordinator(coordinator)
            coordinator.start_workers(uri=uri,
                                      num_processes=config.option.ptmproc_config.num_cores)


# TODO: ############# shutdown ptmproc_runtime in common way


def mproc_pytest_cmdline_main(config, reporter: BasicReporter):
    assert "--as-worker" not in sys.argv
    config.http_session = None
    has_remotes = "--remote-worker" in sys.argv
    config.option.ptmproc_config.server_uri = \
        getattr(config.option, 'mproc_server_uri') or f'127.0.0.1:{find_free_port()}'
    local_proj_file = Path("./ptmproc_project.cfg")
    project_config = getattr(config.option, "project_structure_path", None) or \
        (local_proj_file if local_proj_file.exists() else None)
    if has_remotes:
        remote_clients = config.option.mproc_remote_clients
        version = str(sys.version_info[0]) + "." + str(sys.version_info[1])
        mproc_remote_sys_executable = getattr(config.option, "mproc_remote_sys_executable", None) or \
            f"/usr/bin/python{version}"
        if bool(project_config) != bool(remote_clients):
            # basically if only one is None
            raise pytest.UsageError(f"Must specify both project config and remote clients together or not at all "
                                    f"{project_config} {remote_clients}")
        remote_clients = [remote_clients] if isinstance(remote_clients, str) else remote_clients
        config.option.ptmproc_config.remote_hosts = None if not remote_clients\
            else RemoteWorkerConfig.from_raw_list(remote_clients)
        config.http_session = RemoteWorkerConfig.http_session()
        config.option.ptmproc_config.remote_sys_executable = mproc_remote_sys_executable
    if config.option.ptmproc_config.num_cores < 1:
        raise pytest.UsageError("Number of cores must be 1 or more when running on single host")
    reporter.write(f"Running as main @ {config.option.ptmproc_config.server_uri}\n", green=True)
    default_proj_config = Path(os.getcwd()) / "ptmproc_project.cfg"
    if has_remotes:
        project_config = project_config if (project_config is not None or not default_proj_config.exists())\
            else default_proj_config
    else:
        project_config = None

    orchestrator = Orchestrator(
        project_config=ProjectConfig.from_file(Path(project_config)) if project_config else None,
        uri=config.option.ptmproc_config.server_uri)
    config.ptmproc_runtime = PytestMprocRuntime(mproc_main=orchestrator, coordinator=None)
    if has_remotes:
        # this won't run until run loop when even loop is kicked off :-(
        config.remote_coro = orchestrator.start_remote(remote_workers_config=config.option.ptmproc_config.remote_hosts,
                                                       deploy_timeout=config.option.ptmproc_config.connection_timeout)

    else:
        config.remote_coro = None
        coordinator = orchestrator.start_local(num_processes=config.option.ptmproc_config.num_cores)
        config.ptmproc_runtime.coordinator = coordinator
    config.option.dist = "no"
    val = config.getvalue
    if not val("collectonly"):
        usepdb = config.getoption("usepdb")  # a core option
        if val("dist") != "no" and usepdb:
            raise pytest.UsageError(
                "--pdb is incompatible with distributing tests."
            )  # noqa: E501


def pytest_sessionstart(session):
    with suppress(Exception):
        reporter = session.config.pluginmanager.getplugin('terminalreporter')
        if reporter:
            reporter._session = session


@pytest.mark.trylast
def pytest_configure(config):
    if not config.option.worker and \
            (config.option.ptmproc_config.num_cores is None or getattr(config.option, "mproc_disabled")):
        return  # return of None indicates other hook impls will be executed to do the task at hand
    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
    config.option.dist = "no"


# noinspection PyProtectedMember
def pytest_runtestloop(session):
    if session.config.option.collectonly:
        return
    reporter: Optional[BasicReporter, _pytest.terminal.TerminalReporter] = \
        session.config.pluginmanager.getplugin('terminalreporter')
    if reporter:
        reporter.tests_count = len(session.items)
    if reporter is None:
        reporter = BasicReporter()
    if session.testsfailed and not session.config.option.continue_on_collection_errors:
        raise session.Interrupted("%d errors during collection!!" % session.testsfailed)

    if session.config.option.collectonly:
        return  # should never really get here, but for consistency
    if len(session.items) == 0:
        return
    if not session.config.option.worker and session.config.option.ptmproc_config.num_cores is None \
            or is_degraded() or getattr(session.config.option, "mproc_disabled"):
        # return of None indicates other hook impls will be executed to do the task at hand
        # aka, let the basic hook handle it from here, no distributed processing to be done
        return
    if session.shouldfail:
        if session.config.ptmproc_runtime.coordinator is not None:
            session.config.ptmproc_runtime.coordinator.kill()
        if session.config.ptmproc_runtime.mproc_main is not None:
            session.config.ptmproc_runtime.mproc_main.kill()
        raise session.Failed(session.shouldfail)

    session.config.generated_fixtures = []

    if not hasattr(session.config.option, "fixtures"):
        session.config.option.fixtures = {}
    if not session.config.option.worker \
            and session.config.ptmproc_runtime \
            and session.config.ptmproc_runtime.coordinator:
        for item in session.items:
            if session.shouldfail:
                break
            if hasattr(item, "_request"):
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
                                    raise raise_usage_error(
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
    if not session.config.option.worker \
            and session.config.ptmproc_runtime\
            and session.config.ptmproc_runtime.mproc_main:
        orchestrator = session.config.ptmproc_runtime.mproc_main
        try:
            async def loop():
                if session.config.remote_coro:
                    await session.config.remote_coro
                async with orchestrator:
                    orchestrator.set_items(session.items)
                    await orchestrator.run_loop(session)
            asyncio.get_event_loop().run_until_complete(loop())
        except Exception as e:
            reporter.write(format_exc() + "\n", red=True)
            reporter.write(f"\n>>> ERROR in run loop;  unexpected Exception\n {str(e)}\n\n", red=True)
            raise SystemError() from e
    elif session.config.option.worker:
        session.config.option.worker.test_loop(session)
    elif session.config.ptmproc_runtime.coordinator:
        session.config.ptmproc_runtime.coordinator.shutdown()
    return True


@pytest.hookimpl(hookwrapper=True)
def pytest_collection_finish(session) -> None:
    config = session.config
    verbose = config.option.verbose
    if config.option.worker \
            or (config.ptmproc_runtime and not config.ptmproc_runtime.mproc_main):
        config.option.verbose = -2
    if session.config.option.worker:
        session.config.option.worker.pytest_collection_finish(session)
    yield
    config.option.verbose = verbose


def pytest_internalerror(excrepr):
    from pytest_mproc.worker import WorkerSession
    if WorkerSession.singleton():
        return WorkerSession.singleton().pytest_internalerror(excrepr)


@pytest.mark.tryfirst
def pytest_runtest_logreport(report):
    from pytest_mproc.worker import WorkerSession
    if WorkerSession.singleton():
        return WorkerSession.singleton().pytest_runtest_logreport(report)


# noinspection PyUnusedLocal
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    verbose = config.option.verbose
    if config.option.worker:
        config.option.tbstyle = 'no'
        terminalreporter.reportchars = ""
    elif config.ptmproc_runtime and not config.ptmproc_runtime.mproc_main:
        config.option.verbose = -2
    elif config.ptmproc_runtime and not config.ptmproc_runtime.mproc_main:
        config.option.tbstyle = 'no'
        terminalreporter.reportchars = ""
        if user_output.verbose:
            terminalreporter.line(">>> This is a satellite processing node. "
                                  "Please see master node output for actual test summary <<<<<<<<<<<<",
                                  yellow=True)
    yield
    if config.option.worker:
        config.option.verbose = verbose


# noinspection PyUnusedLocal
@pytest.hookimpl(hookwrapper=True)
def pytest_report_header(config, startdir):
    verbose = config.option.verbose
    if config.option.worker \
            or (config.ptmproc_runtime and not config.ptmproc_runtime.mproc_main):
        config.option.verbose = -2
    yield
    config.option.verbose = verbose


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_sessionstart(session) -> None:
    verbose = session.config.option.verbose
    if session.config.option.worker\
            or (session.config.ptmproc_runtime and not session.config.ptmproc_runtime.mproc_main):
        session.config.option.verbose = -2
    yield
    session.config.option.verbose = verbose


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_sessionfinish(session):
    verbose = session.config.option.verbose
    with suppress(Exception):
        fixtures.Node.Manager.shutdown()
    if session.config.option.worker:
        session.config.option.verbose = -2
    if session.config.ptmproc_runtime and not session.config.ptmproc_runtime.mproc_main:
        session.config.option.verbose = -2
    if not session.config.option.worker and session.config.ptmproc_runtime\
            and session.config.ptmproc_runtime.coordinator is not None:
        with suppress(Exception):
            session.config.ptmproc_runtime.coordinator.kill()
    if not session.config.option.worker and session.config.ptmproc_runtime\
            and session.config.ptmproc_runtime.mproc_main is not None:
        orchestrator = session.config.ptmproc_runtime.mproc_main
        orchestrator.output_summary()
        orchestrator.shutdown()
        if session.config.http_session is not None:
            session.config.http_session.end_session_sync()
    yield
    session.config.option.verbose = verbose
    with suppress(Exception):
        fixtures.Global.Manager.stop()


def raise_usage_error(session, msg: str):
    if session.config.ptmproc_runtime.coordinator is not None:
        session.config.ptmproc_runtime.coordinator.kill()
    raise pytest.UsageError(msg)

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
