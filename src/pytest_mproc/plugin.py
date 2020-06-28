"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import getpass
import inspect
import os
import shutil
import tempfile
from contextlib import contextmanager, suppress
from pathlib import Path
from traceback import format_exc

import _pytest

from pytest_mproc import find_free_port

import pytest
import _pytest.terminal
import socket

from multiprocessing import cpu_count
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.main import Orchestrator
from pytest_mproc.utils import is_degraded, BasicReporter


def _get_ip_addr():
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        print(">>> Cannot get ip address of host.  Return 127.0.0.1 (localhost)")
        return "127.0.0.1"


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
    group._addoption(
        "--max-simultaneous-connections",
        dest="mproc_max_simultaneous_connections",
        metavar="mproc_max_simultaneous_connections",
        action="store",
        type=int,
        help="max # of connections allowed at one time to main process, to prevent deadlock from overload"
    )


@pytest.mark.tryfirst
def pytest_cmdline_main(config):
    """
    Called before "true" main routine.  This is to set up config values well ahead of time
    for things like pytest-cov that needs to know we are running distributed

    Mostly taken from other implementations (such as xdist)
    """
    if config.option.collectonly:
        return

    reporter = BasicReporter()
    worker = getattr(config.option, "mproc_worker", None)
    mproc_server_port = getattr(config.option, 'mproc_server_port', None)
    mproc_server_host = _get_ip_addr() if mproc_server_port is not None else "127.0.0.1"
    mproc_client_connect = getattr(config.option, "mproc_client_connect", None)
    if mproc_client_connect and mproc_server_port:
        raise pytest.UsageError("Cannot specify both -as-master and --as-client at same time")
    config.option.mproc_max_simultaneous_connections = 24 if config.option.mproc_max_simultaneous_connections is None \
        else config.option.mproc_max_simultaneous_connections
    config.option.numprocesses = config.option.mproc_numcores  # this is what pycov uses to determine we are distributed
    if config.option.numprocesses and config.option.numprocesses < 0:
        raise pytest.UsageError("Number of cores must be greater than or equal to zero when running as a master")
    if config.option.mproc_max_simultaneous_connections <= 0:
        raise pytest.UsageError("max simultaneous connections must be greater than 0; preferably greater than 9")
    # validation
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        reporter.write(">>>>> no number of cores provided or running in environment unsupportive of parallelized testing, "
              "not running multiprocessing <<<<<\n", yellow=True)
        return
    if worker:
        return

    if mproc_client_connect:
        if config.option.numprocesses < 1:
            raise pytest.UsageError("Number of cores must be 1 or more when running as client")
        try:
            host, port = mproc_client_connect.rsplit(':', maxsplit=1)
        except Exception:
            raise pytest.UsageError("--as-client must be specified in form '<host>:<port>' of the master node")
        try:
            port = int(port)
        except ValueError:
            raise pytest.UsageError("When specifying connection as client, port must be an integer value")

    else:
        if mproc_server_port is not None:
            host, port = mproc_server_host, mproc_server_port  # running distributed
        else:
            if config.option.numprocesses < 1:
                raise pytest.UsageError("Number of cores must be 1 or more when running on single host")
            host, port = "127.0.0.1", find_free_port()  # running localhost only

        config.option.mproc_main = Orchestrator(host=host, port=port or find_free_port())
        BasicReporter().write(f"Started on port {port}")
        reporter.write(f"Running as main @ {host}:{port}\n", green=True)

    if config.option.numprocesses > 0:
        config.option.mproc_coordinator = Coordinator(
            config.option.numprocesses,
            host=host,
            port=port,
            max_simultaneous_connections=config.option.mproc_max_simultaneous_connections)

    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
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
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        return  # return of None indicates other hook impls will be executed to do the task at hand
    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
    config.option.dist = "no"


def process_fixturedef(target, fixturedef, request):
    generated = request.config.generated_fixtures
    if not fixturedef or fixturedef.scope not in ['global', 'node']:
        return
    fixtures = target.fixtures()
    name = fixturedef.argname
    if name not in fixtures:
        fixture = _pytest.fixtures.resolve_fixture_function(fixturedef, request)
        val = fixture(*fixturedef.argnames)
        if inspect.isgenerator(val):
            generated.append(val)
            val = next(val)
        target.put_fixture(name, val)
        target.fixtures()[name] = val


def pytest_runtestloop(session):
    if session.config.option.collectonly:
        return
    reporter: _pytest.terminal.TerminalReporter = session.config.pluginmanager.getplugin('terminalreporter')
    if reporter:
        reporter.tests_count = len(session.items)
    if reporter is None:
        reporter = BasicReporter()
    if session.testsfailed and not session.config.option.continue_on_collection_errors:
        raise session.Interrupted("%d errors during collection" % session.testsfailed)

    if session.config.option.collectonly:
        return  # should never really get here, but for consistency
    if len(session.items) == 0:
        return
    if getattr(session.config.option, "mproc_numcores", None) is None or is_degraded() or getattr(session.config.option, "mproc_disabled"):
        # return of None indicates other hook impls will be executed to do the task at hand
        # aka, let the basic hook handle it from here, no distributed processing to be done
        return
    if session.shouldfail:
        if hasattr(session.config.option, "mproc_coordinator"):
            session.config.option.mproc_coordinator.kill()
        if hasattr(session.config.option, "mproc_main"):
            session.config.option.mproc_main.kill()
        raise session.Failed(session.shouldfail)

    session.config.generated_fixtures = []

    if hasattr(session.config.option, "mproc_coordinator"):
        coordinator = session.config.option.mproc_coordinator
        for item in session.items:
            if session.shouldfail:
                break
            if hasattr(item, "_request"):
                for name in item._fixtureinfo.argnames:
                    try:
                        fixturedef = item._fixtureinfo.name2fixturedefs.get(name, [None])[0]
                        if not fixturedef or fixturedef.scope != 'node':
                            continue
                        if coordinator and name not in coordinator.fixtures():
                            for argname in fixturedef.argnames:
                                fixdef = item._fixtureinfo.name2fixturedefs.get(argname, [None])[0]
                                process_fixturedef(coordinator, fixdef, item._request)

                            process_fixturedef(coordinator, fixturedef, item._request)
                    except Exception as e:
                        session.shouldfail = f"Exception in fixture: {e.__class__.__name__} raised with msg '{str(e)}'" + \
                            f"{format_exc()}"
                        reporter.write(f">>> Fixture ERROR: {format_exc()}\n", red=True)
                        break

    if hasattr(session.config.option, "mproc_main"):
        orchestrator = session.config.option.mproc_main
        for item in session.items:
            if session.shouldfail:
                break
            if hasattr(item, "_request"):
                for name in item._fixtureinfo.argnames:
                    try:
                        fixturedef = item._fixtureinfo.name2fixturedefs.get(name, [None])[0]
                        if not fixturedef or fixturedef.scope != 'global':
                            continue
                        if orchestrator and name not in orchestrator.fixtures():
                            for argname in fixturedef.argnames:
                                fixdef = item._fixtureinfo.name2fixturedefs.get(argname, [None])[0]
                                process_fixturedef(orchestrator, fixdef, item._request)

                            process_fixturedef(orchestrator, fixturedef, item._request)
                    except Exception as e:
                        session.shouldfail = f"Exception in fixture: {e.__class__.__name__} raised with msg '{str(e)}'" + \
                            f"{format_exc()}"
                        reporter.write(f">>> Fixture ERROR: {format_exc()}\n", red=True)
                        break
        try:
            orchestrator.set_items(session.items)
            orchestrator.run_loop(session)
        except Exception as e:
            reporter.write(format_exc() + "\n", red=True)
            reporter.write(f"\n>>> ERROR in run loop;  unexpected Exception\n {str(e)}\n\n", red=True)
            return False
    if hasattr(session.config.option, "mproc_worker"):
        session.config.option.mproc_worker.test_loop(session)

    return True


def pytest_terminal_summary(terminalreporter: _pytest.terminal.TerminalReporter, exitstatus, config):
    if config.option.collectonly:
        return
    if not hasattr(config.option, "mproc_main") and getattr(config.option, "numprocesses", None):
        terminalreporter.line(">>>>>>>>>> This is a satellite processing node. "
                              "Please see master node output for actual test summary <<<<<<<<<<<<",
                              yellow=True)


def pytest_sessionfinish(session):
    if session.config.option.collectonly:
        return
    if hasattr(session.config.option, "mproc_coordinator"):
        session.config.option.mproc_coordinator.kill()


################
# Process-safe temp dir
###############


class TmpDirFactory:

    """
    tmpdir is not process/thread safe when used in a multiprocessing environment.  Failures on setup can
    occur (even if infrequently) under certain rae conditoins.  This provides a safe mechanism for
    creating temporary directories utilizng s a global-scope fixture
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

