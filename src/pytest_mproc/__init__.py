import inspect
import socket
import sys
from contextlib import closing

import pytest

from pytest_mproc.data import ResourceUtilization
from pytest_mproc.data import GroupTag, DEFAULT_PRIORITY


def priority(level: int):

    def decorator_priority(object):
        if inspect.isclass(object):
            for method in [getattr(object, m) for m in dir(object) if
                           inspect.isfunction(getattr(object, m)) and m.startswith('test')]:
                if not hasattr(method, "_pytest_priority"):
                    method._pytest_priority = level
        elif inspect.isfunction(object):
            object._pytest_priority = level
        else:
            raise Exception("group decorator can only decorate class or function object")
        return object

    return decorator_priority


def group(tag: GroupTag):
    """
    Decorator for grouping tests or a class of tests
    :param name: unique name for group of tests to be serialized under execution
    """
    def decorator_group(object):
        if inspect.isclass(object):
            for method in [getattr(object, m) for m in dir(object) if inspect.isfunction(getattr(object, m)) and m.startswith('test')]:
                method._pytest_group = tag
        elif inspect.isfunction(object):
            object._pytest_group = tag
        else:
            raise Exception("group decorator can only decorate class or function object")
        return object
    return decorator_group


def resource_utilization(time_span: float, start_rusage, end_rusage) -> ResourceUtilization:
    if time_span <= 0.001:
        return -1, -1, -1, end_rusage.ru_maxrss
    if sys.platform.lower() == 'darwin':
        # OS X is in bytes
        delta_mem = (end_rusage.ru_maxrss - start_rusage.ru_maxrss) / 1000.0
    else:
        delta_mem = (end_rusage.ru_maxrss - start_rusage.ru_maxrss)
    ucpu_secs = end_rusage.ru_utime - start_rusage.ru_utime
    scpu_secs = end_rusage.ru_stime - start_rusage.ru_stime
    return ResourceUtilization(
        time_span,
        (ucpu_secs / time_span) * 100.0,
        (scpu_secs / time_span) * 100.0,
        delta_mem)


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


AUTHKEY = __file__[:32].encode('utf-8')


def fixture(fixturefunction, *, scope, **kargs):
    from _pytest.fixtures import fixture
    return fixture(fixturefunction, scope=scope, **kargs)
