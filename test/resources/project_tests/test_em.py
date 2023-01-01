from pytest_mproc.fixtures import global_fixture, node_fixture
import time

global_number = 42


@node_fixture()
def node_fix():
    global global_number
    yield global_number
    global_number += 1


global_number2 = -42

@global_fixture()
def glob_fix():
    global global_number2
    yield global_number2
    global_number2 -= 1


def test1(node_fix, glob_fix, shield_always):
    time.sleep(0.5)
    assert node_fix == 42
    assert glob_fix == -42


def test2(shield_always):
    time.sleep(0.5)
    assert False, "This is supposed to fail"
