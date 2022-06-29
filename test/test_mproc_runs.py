import os
import sys
import time
from pathlib import Path
base_path = Path(__file__).parent
sys.path.insert(0, str(base_path))
sys.path.insert(0, str(base_path.parent / "testsrc"))

import pytest_mproc.plugin
# to pick up dummy_src (ensure in path):
from pytest_mproc import GroupTag
from pytest_mproc.constants import DEFAULT_PRIORITY
from pytest_mproc.data import TestExecutionConstraint

import pytest
from testcode.testsomething import Something

os.environ["PYTEST_MPROC_LAZY_CLEANUP"] = "1"  # only cleanup tempdirs at end of run


group = GroupTag(name="group", priority=DEFAULT_PRIORITY-1, restrict_to=TestExecutionConstraint.SINGLE_NODE)
class_group = GroupTag(name="class_group", restrict_to=TestExecutionConstraint.SINGLE_PROCESS)


@pytest.fixture()
def some_fixture():
    return 1


@pytest_mproc.group(group)
def test_group_m2():
    Something().group_m1()
    assert Something.proc_id == os.getpid()


@pytest_mproc.group(class_group)
class TestGrouped:

    proc_id = None

    @pytest.mark.skip(reason="not implemented yet")
    def test2(self):
        assert TestGrouped.proc_id == os.getpid()

    @pytest_mproc.priority(DEFAULT_PRIORITY - 11)
    def test1(self):
        TestGrouped.proc_id = os.getpid()


@pytest.mark.skip(reason="testing")
def test_some_alg1(global_fix, queue_fixture):
    assert global_fix == 42
    Something().some_alg1()


@pytest.mark.parametrize('data', ['a%s' % i for i in range(400)])
def test_some_alg2(data, some_fixture, global_fix):
    time.sleep(0.2)
    assert global_fix == 42
    Something().some_alg2(data)


@pytest_mproc.group(group)
def test_some_alg3():
    Something().some_alg3()
