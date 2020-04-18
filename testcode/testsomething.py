import os
import pytest_mproc
import sys
# to pick up dummy_src (ensure in path):
sys.path.append(os.path.basename(__file__))

import pytest

from testcode.dummy_src.something import to_be_run_under_test


class Something:

    proc_id = None

    @pytest_mproc.group("group")
    def group_m1(self):
        Something.proc_id = os.getpid()

    def some_alg2(self, data):
        to_be_run_under_test()

    def some_alg1(self):
        assert True

    def some_alg3(self):
        to_be_run_under_test()
        assert False
