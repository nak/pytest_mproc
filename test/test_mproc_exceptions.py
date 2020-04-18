from pytest_mproc.utils import global_initializer, global_finalizer, global_session_context


def test_exception_causes_failure():
    raise Exception("Expected exception from test")


@global_finalizer()
def finalizer_with_exception():
    raise Exception("Expected exception from global finalizer")


@global_session_context()
class TestSessionContextWithExceptionOnExit:

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise Exception("Expected exception from global session context exit")

