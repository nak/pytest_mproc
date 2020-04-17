import os
import pytest_mproc
import sys
# to pick up dummy_src (ensure in path):
sys.path.append(os.path.basename(__file__))

import pytest

from dummy_src.something import to_be_run_under_test
from pytest_mproc_utils import pytest_mproc_global_finalizer, pytest_mproc_global_initializer


class TestSomething:

    proc_id = None

    @pytest_mproc.group("group")
    def test_group_m1(self):
        TestSomething.proc_id = os.getpid()

    @pytest.mark.parametrize('data', ['a%s' % i for i in range(1000)])
    def test_some_alg2(self, data):
        to_be_run_under_test()

    def test_some_alg1(self):
        assert True

    def test_some_alg3(self):
        to_be_run_under_test()
        assert False


@pytest_mproc.group("group")
def test_group_m2():
    assert TestSomething.proc_id == os.getpid()


@pytest_mproc.group("class_group")
class TestGrouped:

    proc_id = None

    def test1(self):
        TestGrouped.proc_id = os.getpid()

    def test2(self):
        assert TestGrouped.proc_id == os.getpid()


@pytest_mproc_global_finalizer
def global_fixture():
    print("\n###### FINALIZE ############\n")


@pytest_mproc_global_initializer
def global_fixture():
    print("\n###### INITIALIZE ############\n")
