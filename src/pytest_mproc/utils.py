import os


def is_degraded():
    return os.environ.get("PYTEST_MPROC_DISABLED")
