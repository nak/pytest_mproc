"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import asyncio
import configparser
import os
import sys

import _pytest.terminal
from pytest_mproc.worker import WorkerSession

from pytest_mproc import worker, user_output, Constants, Settings
from pytest_mproc.coordinator import coordinator_main
from pytest_mproc.main import LocalOrchestrator, RemoteOrchestrator
from pytest_mproc.user_output import always_print

import getpass
import shutil
import tempfile
from contextlib import contextmanager, suppress
from pathlib import Path
from traceback import format_exc
from typing import Callable, Optional, Union, Type

from pytest_mproc.ptmproc_data import (
    ModeEnum,
    PytestMprocConfig,
    RemoteWorkerConfig,
    _determine_cli_args, ProjectConfig,
)


import pytest
from pytest_mproc import fixtures

import socket

from multiprocessing import cpu_count
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
    group = parser.getgroup("pytest_mproc", "better distributed testing through multiprocessing")
    _add_option(
        group,
        "--ptmproc_config",
        dest="mproc_config",
        action="store",
        typ=str,
        help_text="Optional location of ini file to load config values"
    )
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
            "--as-worker-node",
            dest="mproc_mgr_ports",
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
    """
    global cmdline_main_called
    reporter = BasicReporter()
    config.ptmproc_http_session = None
    if cmdline_main_called:
        return
    has_remotes = "--remote-worker" in sys.argv
    cmdline_main_called = True
    config_path = getattr(config.option, "mproc_config", None)
    if config_path:
        if os.path.exists(config_path):
            # noinspection PyBroadException
            try:
                parser = configparser.ConfigParser()
                parser.read(config_path)
                username = parser.get('SSH', 'username') if parser.has_option('SSH', 'username') else None
                password = parser.get('SSH', 'password') if parser.has_option('SSH', 'password') else None
                if username is not None:
                    Settings.set_ssh_credentials(username, password)
                else:
                    always_print("No ssh configuration to process in config.")
                cache_root = parser.get('Directories', 'cache') if parser.has_option('Directories', 'cache') else None
                if cache_root is not None:
                    Settings.set_cache_dir(Path(cache_root))
                temp_root = parser.get('Directories', 'temp') if parser.has_option('Directories', 'temp') else None
                if temp_root is not None:
                    Settings.set_tmp_root(Path(temp_root))
            except Exception:
                always_print(
                    f"File {config_path} improperly formatted and cannot read as ini file",
                    as_error=True
                )
        else:
            always_print(
                f"File {config_path} specified on command line does not exist.  Ignoring read of configuration",
                as_error=True
            )
    mproc_connection_timeout = getattr(config.option, "mproc_connection_timeout", None)
    mproc_disabled = getattr(config.option, "mproc_disabled")
    mproc_num_cores = getattr(config.option, "mproc_numcores", None)
    mproc_mgr_ports = getattr(config.option, "mproc_mgr_ports", None)
    mproc_remote_clients = getattr(config.option, "mproc_remote_clients", None)
    if config.option.mproc_verbose is True:
        user_output.is_verbose = config.option.mproc_verbose
    config.ptmproc_worker = None
    config.ptmproc_orchestrator = None
    config.ptmproc_coordinator = None
    config.ptmproc_config = PytestMprocConfig(
        num_cores=mproc_num_cores,
        connection_timeout=mproc_connection_timeout or 60*30,
    )
    if mproc_num_cores is None or is_degraded() or mproc_disabled:
        if '--as-main' in sys.argv or '--as-worker-node' in sys.argv:
            raise pytest.UsageError(
                "Refusing to execute on distributed system without also specifying the number of cores to use")
        reporter.write(
            ">>> no number of cores provided or running in environment that does not support parallelized testing, "
            "not running multiprocessing <<<<<\n", yellow=True)
        return
    if mproc_num_cores <= 0:
        raise pytest.UsageError(f"Invalid value for number of cores: {mproc_num_cores}")
    config.remote_coro = None
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
    if "PTMPROC_WORKER" in os.environ:
        config.ptmproc_config.mode = ModeEnum.MODE_WORKER
        return mproc_pytest_cmdline_worker(config)
    else:
        config.ptmproc_worker = None
    uri = getattr(config.option, 'mproc_server_uri')
    is_local = not has_remotes and uri is None
    if config.ptmproc_config.num_cores < 1:
        raise pytest.UsageError("Number of cores must be 1 or more when running on single host")
    local_proj_file = Path("./ptmproc_project.cfg")
    project_config_path = (getattr(config.option, "project_structure_path", None) or
                           (local_proj_file if local_proj_file.exists() else None))
    project_config_path = Path(project_config_path) if project_config_path is not None else None
    config.ptmproc_config.project_config = \
        ProjectConfig.from_file(project_config_path) if project_config_path else None
    if mproc_mgr_ports:
        config.ptmproc_config.mode = ModeEnum.MODE_COORDINATOR
        try:
            host, ports_text = mproc_mgr_ports.split(':') if ':' in mproc_mgr_ports else None, mproc_mgr_ports
            global_mgr_port, orchestration_port = ports_text.split(',')
            global_mgr_port = int(global_mgr_port)
            orchestration_port = int(orchestration_port)
        except ValueError:
            raise pytest.UsageError(
                "Invalid cmd line option for --as-worker-node:  must be in form of host ip or name and "
                " integer ports: [<host ip/name>:]<global_manager_port>,<orchestration_port>"
            )
        return mproc_pytest_cmdline_coordinator(config, host, global_mgr_port, orchestration_port,
                                                config.ptmproc_config.project_config)
    if is_local:
        config.ptmproc_config.mode = ModeEnum.MODE_LOCAL_MAIN
        # Running locally on local host only:
        config.ptmproc_orchestrator = LocalOrchestrator(project_config=config.ptmproc_config.project_config)
        return mproc_pytest_cmdline_main_local(config.ptmproc_config, config.ptmproc_orchestrator, reporter)
    elif not has_remotes:
        config.ptmproc_config.mode = ModeEnum.MODE_REMOTE_MAIN
        # user has specified running distributed but with explicit bring-up of workers
        # therefore, must specify ports
        global_mgr_port_text, orchestration_port_text = uri.split(',') if uri is not None else None, None
        global_mgr_port = None if global_mgr_port_text is None else int(global_mgr_port_text)
        # noinspection PyTypeChecker
        orchestration_port = None if orchestration_port_text is None else int(orchestration_port_text)
        config.ptmproc_orchestrator = RemoteOrchestrator(
            project_config=config.ptmproc_config.project_config,
            global_mgr_port=global_mgr_port,
            orchestration_port=orchestration_port
        )
        mproc_pytest_cmdline_main_remote(
            config.ptmproc_config, reporter=reporter,
            orchestrator=config.ptmproc_orchestrator,
            remote_clients=None,
        )
        return
    else:
        config.ptmproc_config.mode = ModeEnum.MODE_REMOTE_MAIN
        # running distributed in automated fashion:
        if config.ptmproc_config.project_config is None:
            raise pytest.UsageError("When running remote, a project config file must be specified con command line,"
                                    "or present in current working directory")
        config.ptmproc_orchestrator = RemoteOrchestrator(project_config=config.ptmproc_config.project_config)
        config.remote_coro = mproc_pytest_cmdline_main_remote(
            config.ptmproc_config, reporter=reporter,
            orchestrator=config.ptmproc_orchestrator,
            remote_clients=mproc_remote_clients)
        return


def mproc_pytest_cmdline_worker(config):
    config.ptmproc_config.num_cores = 1
    config.ptmproc_orchestrator = None
    worker.pytest_cmdline_main(config)
    assert config.ptmproc_worker


def mproc_pytest_cmdline_coordinator(config, host: Optional[str], global_mgr_port: int, orchestration_port: int,
                                     proj_config: ProjectConfig):
    assert 'PTMPROC_WORKER' not in os.environ
    config.option.no_summary = True
    config.option.no_header = True
    host = host or _get_ip_addr()
    if host is None:
        raise Exception("Unable to determine ip address/hostname of local machine. Aborting")
    always_print(
        f"Running as coordinator as host {host} and port {global_mgr_port}, {orchestration_port}[{os.getpid()}]"
    )
    _, config.ptmproc_coordinator = coordinator_main(
        global_mgr_port=global_mgr_port,
        orchestration_port=orchestration_port,
        host=host,
        artifacts_path=proj_config.artifacts_path,
        index=1
    )
    args, addl_env = _determine_cli_args()
    config.ptmproc_coordinator.start_workers(config.ptmproc_config.num_cores, args=args, addl_env=addl_env)


def mproc_pytest_cmdline_main_local(config: PytestMprocConfig, orchestrator: LocalOrchestrator, reporter: BasicReporter
                                    ) -> None:
    assert "--as-worker-node" not in sys.argv
    reporter.write(f"Running locally as main\n", green=True)
    orchestrator.start(config.num_cores)


def mproc_pytest_cmdline_main_remote(config: PytestMprocConfig, reporter: BasicReporter,
                                     orchestrator: RemoteOrchestrator, remote_clients):
    assert "--as-worker-node" not in sys.argv
    project_config = config.project_config
    if project_config is None:
        raise pytest.UsageError("When running remote, a project config file must be specified con command line,"
                                "or present in current working directory")
    version = str(sys.version_info[0]) + "." + str(sys.version_info[1])
    if bool(project_config) != bool(remote_clients):
        # basically if only one is None
        raise pytest.UsageError(f"Must specify both project config and remote clients together or not at all "
                                f"{project_config} {remote_clients}")
    mproc_remote_sys_executable = config.remote_sys_executable or f"/usr/bin/python{version}"
    remote_clients = [remote_clients] if isinstance(remote_clients, str) else remote_clients
    config.remote_hosts = None if not remote_clients\
        else RemoteWorkerConfig.from_raw_list(remote_clients)
    config.ptmproc_http_session = RemoteWorkerConfig.http_session()
    config.remote_sys_executable = mproc_remote_sys_executable
    reporter.write(f"Running with remotes as main\n", green=True)
    return orchestrator.start(remote_workers_config=config.remote_hosts,
                              num_cores=config.num_cores)


# noinspection SpellCheckingInspection
def pytest_sessionstart(session):
    with suppress(Exception):
        reporter = session.config.pluginmanager.getplugin('terminalreporter')
        if reporter:
            reporter._session = session


@pytest.mark.trylast
def pytest_configure(config):
    if config.ptmproc_config.mode == ModeEnum.MODE_UNASSIGNED:
        return
    if not config.ptmproc_worker and \
            (config.ptmproc_config.num_cores is None or getattr(config.option, "mproc_disabled")):
        return  # return of None indicates other hook impls will be executed to do the task at hand
    # tell xdist not to run, (and BTW setting num_cores is enough to tell pycov we are distributed)
    config.option.dist = "no"


# noinspection PyProtectedMember,SpellCheckingInspection
def pytest_runtestloop(session):
    if session.config.option.collectonly:
        return
    if len(session.items) == 0:
        return
    if session.config.ptmproc_config.mode == ModeEnum.MODE_UNASSIGNED:
        return
    mode = session.config.ptmproc_config.mode

    worker_ = session.config.ptmproc_worker
    orchestrator = session.config.ptmproc_orchestrator
    if not worker_ and session.config.ptmproc_config.num_cores is None \
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
    if mode in (ModeEnum.MODE_REMOTE_MAIN, ModeEnum.MODE_LOCAL_MAIN):
        orchestrator = session.config.ptmproc_orchestrator

        async def loop():
            if session.config.remote_coro:
                session.config.remote_task = asyncio.create_task(session.config.remote_coro)
            try:
                async with orchestrator:
                    await orchestrator.run_loop(session, session.items)
            except asyncio.exceptions.CancelledError:
                pytest.exit("Tests canceled")
            except Exception as exc:
                reporter.write(f"{exc.__class__.__qualname__}: {exc}", red=True)
                # noinspection SpellCheckingInspection
                session.shouldfail = True
            finally:
                with suppress(Exception):
                    session.config.remote_task.cancel()
                orchestrator.shutdown()
        asyncio.get_event_loop().run_until_complete(loop())
        http_session = RemoteWorkerConfig.http_session()
        if http_session:
            always_print("Shutting down HTTP session...")
            http_session.end_session_sync()
    elif mode == ModeEnum.MODE_WORKER:
        with suppress(Exception):
            session.config.ptmproc_worker.test_loop(session)
    elif mode == ModeEnum.MODE_COORDINATOR:
        session.config.ptmproc_coordinator.wait()
    return True


@pytest.hookimpl(hookwrapper=True)
def pytest_collection_finish(session) -> None:
    if session.config.ptmproc_config.mode == ModeEnum.MODE_UNASSIGNED:
        yield
    else:
        config = session.config
        verbose = config.option.verbose
        if config.ptmproc_worker:
            config.option.verbose = -2
        if session.config.ptmproc_worker:
            with suppress(Exception):
                session.config.ptmproc_worker.pytest_collection_finish(session)
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
    if config.ptmproc_config.mode == ModeEnum.MODE_UNASSIGNED:
        yield
    else:
        verbose = config.option.verbose
        if config.ptmproc_worker:
            config.option.verbose = -2
            config.option.tbstyle = 'no'
            terminalreporter.reportchars = ""
            if user_output.is_verbose:
                terminalreporter.line(">>> This is a satellite processing node. "
                                      "Please see master node output for actual test summary <<<<<<<<<<<<",
                                      yellow=True)
        yield
        if config.ptmproc_worker:
            config.option.verbose = verbose


# noinspection PyUnusedLocal,SpellCheckingInspection
@pytest.hookimpl(hookwrapper=True)
def pytest_report_header(config, startdir):
    if config.ptmproc_config.mode == ModeEnum.MODE_UNASSIGNED:
        yield
    else:
        verbose = config.option.verbose
        if config.ptmproc_worker:
            config.option.verbose = -2
        yield
        config.option.verbose = verbose


# noinspection SpellCheckingInspection
@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_sessionstart(session) -> None:
    if session.config.ptmproc_config.mode == ModeEnum.MODE_UNASSIGNED:
        yield
    else:
        verbose = session.config.option.verbose
        if session.config.ptmproc_worker:
            session.config.option.verbose = -2
        yield
        session.config.option.verbose = verbose


# noinspection SpellCheckingInspection
@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_sessionfinish(session):
    if session.config.ptmproc_config.mode == ModeEnum.MODE_UNASSIGNED:
        yield
    else:
        verbose = session.config.option.verbose
        http_session = RemoteWorkerConfig.http_session()
        if http_session:
            always_print("Shutting down HTTP session...")
            http_session.end_session_sync()
        with suppress(Exception):
            fixtures.Node.Manager.shutdown()
        if session.config.ptmproc_worker:
            session.config.option.verbose = -2
        orchestrator = session.config.ptmproc_orchestrator
        if orchestrator:
            orchestrator.output_summary()
            orchestrator.shutdown()
        yield
        session.config.option.verbose = verbose
        with suppress(Exception):
            fixtures.Global.Manager.stop()


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
