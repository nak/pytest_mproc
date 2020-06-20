import os
import sys
from typing import AnyStr, Any


def is_degraded():
    return os.environ.get("PYTEST_MPROC_DISABLED")


class BasicReporter:

    def write(self, text: AnyStr, *args: Any, **kargs: Any):
        os.write(sys.stderr.fileno(), text.encode('utf-8'))
