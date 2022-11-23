class FatalError(Exception):
    """
    raised to exit pytest immediately
    """


class TestError(BaseException):
    """
    To be thrown when a test setup/test environment issue occurs preventing test execution from even happening

    :param retry: whether to retry the test when raised in the context of pytest-mproc distributed execution.
       if not specified, default is True
    :param fatal: whether error is fatal when raised in the context of pytest-mproc distributed executions;  errors
       marked fatal, meaning that all further processing for the work-node on which exception was raised will be
       stopped. Default is False if not specified
    """

    def __init__(self, msg: str, retry: bool = True, fatal: bool = False):
        super().__init__(msg)
        self._retry = retry
        self._fatal = fatal

    @property
    def retry(self) -> bool:
        return self._retry

    @property
    def fatal(self) -> bool:
        return self._fatal
