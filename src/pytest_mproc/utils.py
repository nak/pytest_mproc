import inspect
import os
from contextlib import contextmanager


def is_degraded():
    return os.environ.get("PYTEST_MPROC_DISABLED")


class _GlobalFixtures:
    count = 0
    finalizers = []
    initializers = []
    session_contexts = []


def global_finalizer(*args):
    """
    Decorator for adding a finalizer called only once and only when all tests complete
    """
    def inner(func):
        _GlobalFixtures.finalizers.append((func, args))
        return func
    return inner


def global_initializer(*args):
    """
    Decorator to add an initializer called once before any tests execute
    """
    def inner(func):
        _GlobalFixtures.initializers.append((func, args))
        return func
    return inner


def global_session_context(*args):

    def wrapper(obj):
        if inspect.isfunction(obj):
            func = contextmanager(obj)
            _GlobalFixtures.session_contexts.append((func, args))
            return func
        elif inspect.isclass(obj) and hasattr(obj, '__enter__') and hasattr(obj, '__exit__'):
            _GlobalFixtures.session_contexts.append((obj, args))
            return obj
        else:
            raise Exception("@pytest_mproc.session_context: object is not a context manager")

    return wrapper
