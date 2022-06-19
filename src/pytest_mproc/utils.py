import os
import sys
from typing import AnyStr, Any


def is_degraded():
    return os.environ.get("PYTEST_MPROC_DISABLED")


class BasicReporter:

    # noinspection PyMethodMayBeStatic
    def write(self, text: AnyStr, *_args: Any, **_kargs: Any):
        os.write(sys.stderr.fileno(), text.encode('utf-8'))
