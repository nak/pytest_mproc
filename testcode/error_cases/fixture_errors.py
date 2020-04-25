import pytest

@pytest.fixture(scope='global')
def proper_report_on_exception_in_fixture():
    raise Exception("Forced exception for test purposes")


def test_report_on_exception(proper_report_on_exception_in_fixture):
    pass  # will raise exception on setup