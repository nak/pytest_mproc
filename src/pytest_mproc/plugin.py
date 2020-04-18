"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import contextlib
import pytest

from multiprocessing import cpu_count
from pytest_mproc.coordinator import Coordinator
from pytest_mproc.utils import _GlobalFixtures, is_degraded


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
    worker = getattr(config.option, "mproc_worker", None)
    if not worker:
        config.context_managers = [(cm(), fixtures) for cm, fixtures in _GlobalFixtures.session_contexts]
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
    session.config.context_managers = [cm for cm, fixtures in session.config.context_managers if not fixtures or
                                       [a for a in fixtures if any([a in item._fixtureinfo.argnames
                                                                    for item in session.items])]] if worker is None else []
    if getattr(session.config.option, "mproc_numcores", None) is None or is_degraded() or getattr(session.config.option, "mproc_disabled"):
        for func, args in _GlobalFixtures.initializers:
            try:
                if not args:
                    func()
                else:
                    for item in session.items:
                        if [a for a in args if a in item._fixtureinfo.argnames]:
                            func()
                            break
            except Exception as e:
                print(f"\n>>>> ERROR in global initializer {func}. Aborting! <<<<<\n {str(e)}\n")
                if hasattr(session.config, 'coordinator'):
                    session.config.coordinator.stop(None)
                return False
        for cm in session.config.context_managers:
            try:
                cm.__enter__()
            except Exception as e:
                print(f"\n>>>> ERROR upon enter of global session context {cm}. Aborting! <<<<<\n {str(e)}\n")
                if hasattr(session.config, 'coordinator'):
                    session.config.coordinator.stop(None)
                return False
        return

    if worker is None:
        for func, args in _GlobalFixtures.initializers:
            try:
                if not args:
                    func()
                else:
                    for item in session.items:
                        if [a for a in args if a in item._fixtureinfo.argnames]:
                            func()
                            break
            except Exception as e:
                print(f">>>> ERROR in global initializer. Aborting! <<<<<\n {str(e)}")
                if hasattr(session.config, 'coordinator'):
                    session.config.coordinator.stop(None)
                return False
    if getattr(session.config.option, "mproc_numcores", None) is None or is_degraded() or getattr(session.config.option, "mproc_disabled"):
        return  # return of None indicates other hook impls will be executed to do the task at hand
    if not session.config.getvalue("collectonly") and worker is None:
        try:
            # main coordinator loop:
            with contextlib.ExitStack() as stack, session.config.coordinator as coordinator:
                try:
                    for cm in session.config.context_managers:
                            stack.enter_context(cm)
                except Exception as e:
                    print(f"\n>>>> ERROR upon entering global session context {cm}:\n {str(e)}\n")
                    return False
                try:
                    coordinator.set_items(session.items)
                    coordinator.run(session)
                except Exception as e:
                    print(f"\n>>> ERROR in run loop;  unexpected Exception\n {str(e)}\n")
                    return False
        except Exception as e:
            print(f"\n>>>> ERROR upon exit of global session context:\n {str(e)}\n")
        finally:
            session.config.coordinator.stop(None)
    else:
        worker.test_loop(session)
    return True


def pytest_sessionfinish(session, *args):
    worker = getattr(session.config.option, "mproc_worker", None)
    if not worker:
        if getattr(session.config.option, "mproc_numcores", None) is None or is_degraded() or getattr(session.config.option,
                                                                                                "mproc_disabled"):
            if hasattr(session.config, "context_managers"):
                for cm in reversed(session.config.context_managers):
                    try:
                        cm.__exit__(None, None, None)  # exceptions are tracked as part of test reporting, not through cm
                    except Exception as e:
                        print(f">>>> ERROR upon exit of global session context {cm}; Ignoring:\n {str(e)}")
        # only executing in main thread and none of the worker threads:
        for func, args in reversed(_GlobalFixtures.finalizers):
            try:
                if not args:
                    func()
                else:
                    for item in session.items:
                        if [a for a in args if a in item._fixtureinfo.argnames]:
                            func()
                            break
            except Exception as e:
                print(f">>>> ERROR in global finalizer {func};  In=gnoring:\n {str(e)}")
