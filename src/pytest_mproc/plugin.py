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
from pytest_mproc.config import MPManagerConfig, RoleEnum
from pytest_mproc.fixtures import Node, Global

import pytest
import _pytest.terminal
import socket

from multiprocessing import cpu_count
from pytest_mproc.coordinator import Coordinator
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
    mproc_client_connect = getattr(config.option, "mproc_client_connect", None)
    if mproc_client_connect and mproc_server_port:
        raise pytest.UsageError("Cannot specify both -as-master and --as-client at same time")
    config.option.numprocesses = config.option.mproc_numcores  # this is what pycov uses to determine we are distributed
    # We are a:
    #   WORKER no matter what if mproc_worker attr is set (per worker_main routine, i.e. started as a worker thread)
    #   MASTER if server port is specified on command line OR if neither server port nor client connection is specified
    #      on command line. In the latter case, the request is simply to run all tests on localhost, not remtoe workers
    #   COORDINATOR if client connect is specified
    role = RoleEnum.WORKER if worker is not None else \
        RoleEnum.MASTER if (mproc_server_port is not None or
                            (mproc_server_port is None and mproc_client_connect is None)) else \
        RoleEnum.COORDINATOR
    if role == RoleEnum.MASTER:
        reporter.write(f"Running as {role}: {_get_ip_addr()}:{mproc_server_port}\n", green=True)

    # validation
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        config.option.mpconfig = MPManagerConfig(
            global_mgr_host='127.0.0.1',
            global_mgr_port=find_free_port(),
            role=role,
            num_processes=0)
        config.mproc_global_manager = Global.Manager(config.option.mpconfig)
        config.mproc_node_manager = Node.Manager(config.option.mpconfig)
        reporter.write(">>>>> no number of cores provided or running in environment unsupportive of parallelized testing, "
              "not running multiprocessing <<<<<\n", yellow=True)
        return
    elif hasattr(config.option, "mpconfig"):
        mpconfig = config.option.mpconfig
    elif mproc_server_port:
        if config.option.numprocesses < 0:
            raise pytest.UsageError("Number of cores must be greater than or equal to zero when running as a master")
        mpconfig = MPManagerConfig(global_mgr_host=_get_ip_addr(),
                                   global_mgr_port=mproc_server_port,
                                   role=role,
                                   num_processes=config.option.mproc_numcores)
    elif mproc_client_connect:
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

        mpconfig = MPManagerConfig(role=role,
                                   global_mgr_host=host,
                                   global_mgr_port=port,
                                   num_processes=config.option.mproc_numcores)
    else:
        if config.option.numprocesses < 1:
            raise pytest.UsageError("Number of cores must be 1 or more when running on a single node")
        mpconfig = MPManagerConfig(role=role,
                                   global_mgr_host='127.0.0.1',
                                   global_mgr_port=find_free_port(),
                                   num_processes=config.option.mproc_numcores)
    config.option.mpconfig = mpconfig
    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
    config.option.dist = "no"
    val = config.getvalue
    if not val("collectonly"):
        usepdb = config.getoption("usepdb")  # a core option
        if val("dist") != "no" and usepdb:
            raise pytest.UsageError(
                "--pdb is incompatible with distributing tests."
            )  # noqa: E501
    config.coordinator = None
    if role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]:
        # in main thread,
        # instantiate coordinator here and start to kick off processing on workers early, so they can
        # process config info in parallel to this thread
        config.coordinator = Coordinator(mpconfig)
    config.mproc_global_manager = Global.Manager(mpconfig)
    config.mproc_node_manager = Node.Manager(mpconfig)
    if role != RoleEnum.WORKER:
        config.coordinator.start(mpconfig)
    elif hasattr(config.option, "connect_sem"):
        config.option.connect_sem.release()


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


@pytest.mark.tryfirst
def pytest_fixture_setup(fixturedef, request):
    my_cache_key = fixturedef.cache_key(request)
    if request.config.option.mpconfig.role == RoleEnum.WORKER:
        if request.config.option.mpconfig.global_fixtures is None:
            client = request.config.mproc_global_manager # Global.Manager(request.config.option.mpconfig, force_as_client=True)
            request.config.option.mpconfig.global_fixtures = client.get_fixtures()
        if request.config.option.mpconfig.node_fixtures is None:
            client = request.config.mproc_node_manager # Node.Manager(request.config.option.mpconfig, force_as_client=True)
            request.config.option.mpconfig.node_fixtures = client.get_fixtures()
        if fixturedef.scope == 'global':
            fixturedef.cached_result = (request.config.option.mpconfig.global_fixtures.get(fixturedef.argname),
                                        my_cache_key,
                                        None)
            return request.config.option.mpconfig.global_fixtures.get(fixturedef.argname)
        elif fixturedef.scope == 'node':
            fixturedef.cached_result = (request.config.option.mpconfig.node_fixtures.get(fixturedef.argname),
                                        my_cache_key,
                                        None)
            return request.config.option.mpconfig.node_fixtures.get(fixturedef.argname)
        else:
            return

    if not getattr(request.config.option, "mproc_numcores") or fixturedef.scope not in ['node', 'global']:
        return
    if not hasattr(request.config, "generated_fixtures"):
        request.config.generated_fixtures = []
    if request.config.option.mpconfig.global_fixtures is None:
        request.config.option.mpconfig.global_fixtures = {}
    if request.config.option.mpconfig.node_fixtures is None:
        request.config.option.mpconfig.node_fixtures = {}
    if fixturedef.scope == 'node':
        assert request.config.option.mpconfig.node_fixtures.get(fixturedef.argname) is not None
        fixturedef.cached_result = (request.config.option.mpconfig.node_fixtures.get(fixturedef.argname),
                                    my_cache_key,
                                    None)
        return fixturedef.cached_result[0]
    elif fixturedef.scope == 'global':
        assert request.config.option.mpconfig.global_fixtures.get(fixturedef.argname) is not None
        fixturedef.cached_result = (request.config.option.mpconfig.global_fixtures.get(fixturedef.argname),
                                    my_cache_key,
                                    None)
        return fixturedef.cached_result[0]


def process_fixturedef(fixturedef, request):
    generated = request.config.generated_fixtures
    global_fixtures = request.config.option.mpconfig.global_fixtures
    node_fixtures = request.config.option.mpconfig.node_fixtures
    config = request.config
    mpconfig: MPManagerConfig = config.option.mpconfig
    if not fixturedef or fixturedef.scope not in ['global', 'node']:
        return
    name = fixturedef.argname
    assert mpconfig.role != RoleEnum.WORKER
    if mpconfig.role == RoleEnum.MASTER and fixturedef.scope == 'global' and name not in global_fixtures:
        global_fixtures[name] = _pytest.fixtures.resolve_fixture_function(fixturedef, request)
        val = global_fixtures[name](*fixturedef.argnames)
        if inspect.isgenerator(val):
            generated.append(val)
            val = next(val)
        mpconfig.global_fixtures[name] = val
    if mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR] and fixturedef.scope == 'node'\
            and name not in node_fixtures:
        node_fixtures[name] = _pytest.fixtures.resolve_fixture_function(fixturedef, request)
        val = node_fixtures[name](*fixturedef.argnames)
        if inspect.isgenerator(val):
            generated.append(val)
            val = next(val)
        mpconfig.node_fixtures[name] = val


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
        if session.config.option.mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]:
            if hasattr(session.config, "coordinator"):
                session.config.coordinator.kill()
        return  # should never really get here, but for consistency
    if len(session.items) == 0:
        return
    if getattr(session.config.option, "mproc_numcores", None) is None or is_degraded() or getattr(session.config.option, "mproc_disabled"):
        # return of None indicates other hook impls will be executed to do the task at hand
        # aka, let the basic hook handle it from here, no distributed processing to be done
        return
    mpconfig = session.config.option.mpconfig
    session.config.generated_fixtures = []
    if mpconfig.role != RoleEnum.WORKER:
        if mpconfig.global_fixtures is None:
            mpconfig.global_fixtures = {}
        if mpconfig.node_fixtures is None:
            mpconfig.node_fixtures = {}
        for item in session.items:
            if session.shouldfail:
                break
            if hasattr(item, "_request"):
                for name in item._fixtureinfo.argnames:
                    try:
                        if name not in mpconfig.global_fixtures and name not in mpconfig.node_fixtures:
                            fixturedef = item._fixtureinfo.name2fixturedefs.get(name, [None])[0]
                            if not fixturedef:
                                continue
                            for argname in fixturedef.argnames:
                                fixdef = item._fixtureinfo.name2fixturedefs.get(argname, [None])[0]
                                process_fixturedef(fixdef, item._request)

                            process_fixturedef(fixturedef, item._request)
                    except Exception as e:
                        session.shouldfail = f"Exception in fixture: {e.__class__.__name__} raised with msg '{str(e)}'" + \
                            f"{format_exc()}"
                        reporter.write(f">>> Fixture ERROR: {format_exc()}\n", red=True)
                        break

    if session.shouldfail:
        if mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]:
            if hasattr(session.config, "coordinator"):
                session.config.coordinator.kill()
        raise session.Failed(session.shouldfail)
    elif mpconfig.role != RoleEnum.WORKER:
        try:
            if mpconfig.role == RoleEnum.MASTER:
                session.config.mproc_global_manager.register_fixtures(mpconfig.global_fixtures)
            session.config.mproc_node_manager.register_fixtures(mpconfig.node_fixtures)
        except Exception as e:
            import pickle
            for i, v in mpconfig.node_fixtures.items():
                reporter.write(f">>> {i}\n", red=True)
                reporter.write(f">>> {pickle.dumps(v)}\n", red=True)
            session.shouldfail = f"Exception in fixture: {e.__class__.__name__} raised with msg '{str(e)}'" + \
                f"{format_exc()}\n {mpconfig.node_fixtures}"
            reporter.write(f">>>> Error registering node/global level fixture(s): {str(e)}\n", red=True)
            if hasattr(session.config, "coordinator"):
                session.config.coordinator.kill()
            raise session.Failed(session.shouldfail)

    if mpconfig.role != RoleEnum.WORKER:
        if not session.config.getvalue("collectonly"):
            # main coordinator loop:
            with session.config.coordinator as coordinator:
                try:
                    coordinator.set_items(session.items)
                    coordinator.run(session)
                except Exception as e:
                    reporter.write(format_exc() + "\n", red=True)
                    reporter.write(f"\n>>> ERROR in run loop;  unexpected Exception\n {str(e)}\n\n", red=True)
                    return False
    elif mpconfig.role == RoleEnum.WORKER:
        worker = getattr(session.config.option, "mproc_worker")
        worker.test_loop(session)

    return True


def pytest_terminal_summary(terminalreporter: _pytest.terminal.TerminalReporter, exitstatus, config):
    if config.option.collectonly:
        return
    if config.option.mpconfig.role != RoleEnum.MASTER:
        terminalreporter.line(">>>>>>>>>> This is a satellite processing node. "
                              "Please see master node output for actual test summary <<<<<<<<<<<<",
                              yellow=True)


def pytest_sessionfinish(session):
    reporter = BasicReporter()
    if session.config.option.collectonly:
        return
    mpconfig = session.config.option.mpconfig
    if mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]:
        if hasattr(session.config, "coordinator"):
            session.config.coordinator.kill()
    if mpconfig.role == RoleEnum.MASTER:
        try:
            session.config.mproc_global_manager.shutdown()
        except Exception as e:
            reporter.write(">>> INTERNAL Error shutting down mproc manager\n", red=True)
    if mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]:
        try:
            session.config.mproc_node_manager.shutdown()
        except Exception as e:
            reporter.write(">>> INTERNAL Error shutting down mproc manager\n", red=True)
    generated = getattr(session.config, "generated_fixtures", [])
    errors = []
    for item in generated:
        try:
            try:
                next(item)
                raise Exception(f"Fixture generator did not stop {item}")
            except StopIteration:
                pass
        except Exception as e:
            reporter.write(f">>> Error in exit of fixture context {e}\n", red=True)
            errors.append(e)

    if errors:
        raise pytest.UsageError(">>> One or more global or node-level fixtures threw an Exception on exit from context") \
            from errors[0]

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
