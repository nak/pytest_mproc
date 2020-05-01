"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import getpass
import inspect
import os
import shutil
import socket
import tempfile
import traceback
from contextlib import contextmanager
from pathlib import Path

import _pytest

from pytest_mproc import find_free_port
from pytest_mproc.config import MPManagerConfig, RoleEnum
from pytest_mproc.fixtures import Node, Global

import pytest
import _pytest.terminal

from multiprocessing import cpu_count
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.utils import is_degraded


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
        print(f"Running as {role}: {socket.gethostbyname(socket.gethostname())}:{mproc_server_port}")

    # validation
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        config.option.mpconfig = MPManagerConfig(
            global_mgr_host='127.0.0.1',
            global_mgr_port=find_free_port(),
            role=role,
            num_processes=0)
        config.mproc_global_manager = Global.Manager(config.option.mpconfig)
        config.mproc_node_manager = Node.Manager(config.option.mpconfig)
        print(">>>>> no number of cores provided or running in environment unsupportive of parallelized testing, "
              "not running multiprocessing <<<<<")
        return
    elif hasattr(config.option, "mpconfig"):
        mpconfig = config.option.mpconfig
    elif mproc_server_port:
        if config.option.numprocesses < 0:
            raise pytest.UsageError("Number of cores must be greater than or equal to zero when running as a master")
        mpconfig = MPManagerConfig(global_mgr_host=socket.gethostbyname(socket.gethostname()),
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
    if role != RoleEnum.WORKER:
        config.coordinator.start(mpconfig)
    config.mproc_node_manager = Node.Manager(mpconfig)


@pytest.mark.trylast
def pytest_configure(config):
    if getattr(config.option, "mproc_numcores", None) is None or is_degraded() or getattr(config.option, "mproc_disabled"):
        return  # return of None indicates other hook impls will be executed to do the task at hand
    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
    config.option.dist = "no"


@pytest.mark.tryfirst
def pytest_fixture_setup(fixturedef, request):
    if fixturedef.scope == 'node':
        param_index = request.param_index if not hasattr(request, "param") else request.param
        return request.config.mproc_node_manager.get_fixturedef(fixturedef, param_index)
    elif fixturedef.scope == 'global':
        return request.config.mproc_global_manager.get_fixturedef(fixturedef, request)
    return _pytest.fixtures.pytest_fixture_setup(fixturedef, request)


def process_fixturedef(name: str, item, session, global_fixtures, node_fixtures):
    try:
        mpconfig = session.config.option.mpconfig
        request = item._request
        fixturedef = item._fixtureinfo.name2fixturedefs.get(name, None)
        generated = []
        if mpconfig.role == RoleEnum.MASTER and fixturedef and fixturedef[0].scope == 'global' and name not in global_fixtures:
            global_fixtures[name] = _pytest.fixtures.resolve_fixture_function(fixturedef[0], request)
            val = global_fixtures[name](*fixturedef[0].argnames)
            if inspect.isgenerator(val):
                generated.append(val)
                val = next(val)
            try:
                session.config.mproc_global_manager.register_fixture(name, val)
            except TypeError as e:
                request.config.mproc_global_manager.register_fixture(name, e)
                session.should_fail = f">>> Cannot pickle global fixture object: {e}"
            except Exception as e:
                request.config.mproc_global_manager.register_fixture(name, e)
                session.should_fail = f">>> Exception in multiprocess communication: {e}"
        if mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR] and fixturedef and \
                fixturedef[0].scope == 'node' and name not in node_fixtures:
            node_fixtures[name] = _pytest.fixtures.resolve_fixture_function(fixturedef[0], request)
            val = node_fixtures[name](*fixturedef[0].argnames)
            if inspect.isgenerator(val):
                generated.append(val)
                val = next(val)
            try:
                session.config.mproc_node_manager.register_fixture(name, val)
            except TypeError as e:
                request.config.mproc_node_manager.register_fixture(name, e)
                session.should_fail = f">>> Cannot pickle global fixture object: {e}"
            except Exception as e:
                request.config.mproc_node_manager.register_fixture(name, e)
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
    mpconfig = session.config.option.mpconfig
    if session.testsfailed and not session.config.option.continue_on_collection_errors:
        raise session.Interrupted("%d errors during collection" % session.testsfailed)

    if session.config.option.collectonly:
        return  # should never really get here, but for consistency

    session.generated_fixtures = []
    if mpconfig.role != RoleEnum.WORKER:
        for item in session.items:
            if session.shouldfail:
                break
            if hasattr(item, "_request"):

                for name in item._fixtureinfo.argnames:
                    if name not in global_fixtures and name not in node_fixtures:
                        session.generated_fixtures += process_fixturedef(name, item, session,
                                                                          global_fixtures, node_fixtures)
                    if session.shouldfail:
                        break
    if session.shouldfail:
        print(f">>> Fixture ERROR: {session.shouldfail}")
        if mpconfig.role == RoleEnum.MASTER:
            session.config.mproc_global_manager.release(session.Failed(session.shouldfail))
        if mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]:
            session.config.mproc_node_manager.release(session.Failed(session.shouldfail))
            if hasattr(session.config, "coordinator"):
                session.config.coordinator.kill()
        raise session.Failed(session.shouldfail)
    # signals any workers waiting on global fixture(s) to continue
    if mpconfig.role == RoleEnum.MASTER:
        session.config.mproc_global_manager.release(True)
    if mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]:
        session.config.mproc_node_manager.release(True)
    if getattr(session.config.option, "mproc_numcores", None) is None or is_degraded() or getattr(session.config.option, "mproc_disabled"):
        # return of None indicates other hook impls will be executed to do the task at hand
        # aka, let the basic hook handle it from here, no distributed processing to be done
        return

    if mpconfig.role != RoleEnum.WORKER:
        if not session.config.getvalue("collectonly"):
            # main coordinator loop:
            with session.config.coordinator as coordinator:
                try:
                    coordinator.set_items(session.items)
                    coordinator.run(session)
                except Exception as e:
                    import traceback
                    print(traceback.format_exc())
                    print(f"\n>>> ERROR in run loop;  unexpected Exception\n {str(e)}\n")
                    return False
    elif mpconfig.role == RoleEnum.WORKER:
        worker = getattr(session.config.option, "mproc_worker")
        worker.test_loop(session)

    return True


def pytest_terminal_summary(terminalreporter: _pytest.terminal.TerminalReporter, exitstatus, config):
    if config.option.mpconfig.role != RoleEnum.MASTER:
        terminalreporter.line(">>>>>>>>>> This is a satellite processing node. "
                              "Please see master node output for actual test summary <<<<<<<<<<<<",
                              yellow=True)


def pytest_sessionfinish(session):
    mpconfig = session.config.option.mpconfig
    if mpconfig.role == RoleEnum.MASTER:
        try:
            session.config.mproc_global_manager.shutdown()
        except Exception as e:
            print(">>> INTERNAL Error shutting down mproc manager")
    if mpconfig.role in [RoleEnum.MASTER, RoleEnum.COORDINATOR]:
        try:
            session.config.mproc_node_manager.shutdown()
        except Exception as e:
            print(">>> INTERNAL Error shutting down mproc manager")
    generated = getattr(session, "generated_fixtures", [])
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
