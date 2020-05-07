import os
from pathlib import Path

import pytest_mproc
import sys
# to pick up dummy_src (ensure in path):
sys.path.append(os.path.basename(__file__))
sys.path.append(os.path.join(os.path.basename(__file__), "..", "testcode"))

import pytest
from testcode.testsomething import Something

os.environ["PYTEST_MPROC_LAZY_CLEANUP"] = "1"  # only cleanup tempdirs at end of run


@pytest.fixture()
def some_fixture():
    return 1


@pytest_mproc.group("group", 1)
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


def test_some_alg1(global_fix, mp_tmpdir_factory, queue_fixture):
    assert global_fix == 42
    Something().some_alg1()


@pytest.mark.parametrize('data', ['a%s' % i for i in range(1000)])
def test_some_alg2(data, some_fixture, global_fix, mp_tmp_dir: Path):
    unique_path = str(mp_tmp_dir.joinpath("unique_file.txt"))
    assert not os.path.exists(unique_path),  "mp_tmp_dir did not produce a unique path as expected"
    with open(unique_path, 'w'):
        pass
    assert global_fix == 42
    Something().some_alg2(data)


def test_some_alg3():
    Something().some_alg3()

