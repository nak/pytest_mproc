import os
import pytest_mproc
import sys
# to pick up dummy_src (ensure in path):
sys.path.append(os.path.basename(__file__))
sys.path.append(os.path.join(os.path.basename(__file__), "..", "testcode"))

import pytest
from testcode.testsomething import Something


@pytest.fixture()
def some_fixture():
    return 1


@pytest_mproc.group("group")
def test_group_m2():
    Something().group_m1()
    assert Something.proc_id == os.getpid()


@pytest_mproc.group("class_group")
class TestGrouped:

    proc_id = None

    def test1(self):
        TestGrouped.proc_id = os.getpid()

    def test2(self):
        assert TestGrouped.proc_id == os.getpid()


def test_some_alg1(global_fix):
    assert global_fix == 42
    Something().some_alg1()


@pytest.mark.parametrize('data', ['a%s' % i for i in range(1000)])
def test_some_alg2(data, some_fixture, global_fix):
    assert global_fix == 42
    Something().some_alg2(data)


def test_some_alg3():
    Something().some_alg3()

