import pytest

@pytest.fixture(scope='session')
def fixture_session():
    return 12


@pytest.fixture(scope='global')
def fixture_global_wrong_dependency(fixture_session):
    pass  # will cause setup error with dependency on session fixture


def test_it(fixture_global_wrong_dependency):
    pass
