from pytest_mproc.utils import global_initializer, global_finalizer, global_session_context


@global_initializer()
def initializer_with_exception():
    raise Exception("Expected exception from global initialzier")


def test_something():
    pass
