"""
This file contains some standard pytest_* plugin hooks to implement multiprocessing runs
"""
import os

import pytest

from multiprocessing import cpu_count
from pytest_mproc.coordinator import Coordinator
from pytest_mproc_utils import _GlobalFixtures


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
            "-C",
            "--cores",
            dest="numcores",
            metavar="numcores",
            action="store",
            type=parse_numprocesses,
            help="you can use 'auto' here to set to the number of  CPU cores on host system",
        )


@pytest.mark.tryfirst
def pytest_cmdline_main(config):
    """
    Called before "true" main routine.  This is to set up config values well ahead of time
    for things like pytest-cov that needs to know we are running distributed

    Mostly taken from other implementations (such as xdist)
    """
    if getattr(config.option, "numcores", None) is None:
        # xdist is configured, so don't do anything
        print(">>>>> no number of cores provided, not running multiprocessing <<<<<")
        return
    config.option.numprocesses = config.option.numcores  # this is what pycov uses to determine we are distributed
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


@pytest.mark.try_last
def pytest_configure(config):
    if getattr(config.option, "numcores", None) is None:
        return  # return of None indicates other hook impls will be executed to do the task at hand
    # tell xdist not to run, (and BTW setting numprocesses is enough to tell pycov we are distributed)
    config.option.dist = "no"
    worker = getattr(config.option, "mproc_worker", None)
    if not worker:
        # in main thread,
        # instantiate coordinator here and start to kick off processing on workers early, so they can
        # process config info in parallel to this thread
        config.coordinator = Coordinator(config.option.numcores)
        config.coordinator.start()


@pytest.mark.tryfirst
def pytest_runtestloop(session):
    if getattr(session.config.option, "numcores", None) is None:
        for func in _GlobalFixtures.initializers:
            func()
        return  # return of None indicates other hook impls will be executed to do the task at hand
    worker = getattr(session.config.option, "mproc_worker", None)
    if not session.config.getvalue("collectonly") and worker is None:
        # main coordinator loop:
        with session.config.coordinator as coordinator:
            coordinator.set_items(session.items)
            coordinator.run(session)
    else:
        worker.test_loop(session)
    return True


def pytest_sessionfinish(session, *args):
    funcs = _GlobalFixtures.finalizers.get(os.getpid(), [])
    worker = getattr(session.config.option, "mproc_worker", None)
    if not worker:
        # only execute in main thread and none of the worker threads:
        for func in funcs:
            func()
