import os


class _GlobalFixtures:
    count = 0
    finalizers = {}  # by pid
    initializers = []


def pytest_mproc_global_finalizer(func):
    """
    Decorator for adding a finalizer called only once and only when all tests complete
    """
    _GlobalFixtures.finalizers.setdefault(os.getpid(), []).append(func)
    return func


def pytest_mproc_global_initializer(func):
    """
    Decorator to add an initializder called once before any tests execute
    """
    _GlobalFixtures.initializers.append(func)
    return func
