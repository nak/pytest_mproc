from pytest_mproc.utils import global_session_context


@global_session_context()
class TestSessionContextWithExceptionOnEnter:

    def __enter__(self):
        raise Exception("Expected exception from global session context enter")

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def test_something_else():
    pass
