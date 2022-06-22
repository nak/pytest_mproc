from dataclasses import dataclass
from enum import Enum
from typing import List, Union, Optional

from pytest_mproc import DEFAULT_PRIORITY

# noinspection PyBroadException
try:
    from pytest import TestReport
except Exception:
    from _pytest.reports import TestReport


class TestExecutionConstraint(Enum):
    """
    Constrain mode of grouped tests to run on only a single node or within only a single process on a node
    """
    __test__ = False  # tell pytest not to treat as test class
    SINGLE_PROCESS = "SINGLE_PROCESS"
    SINGLE_NODE = "SINGLE_NODE"


@dataclass
class TestBatch:
    """
    A group of tests to run together on same node or serially within same process
    """
    test_ids: List[str]
    # list of test ids belonging to batch
    priority: int = DEFAULT_PRIORITY
    # priority of the batch of tests (overridden by any priority set on an individual test)
    restriction: TestExecutionConstraint = TestExecutionConstraint.SINGLE_PROCESS
    # restriction on mode of execution


@dataclass
class ResourceUtilization:
    """
    Hold resource utilization measurements
    """
    time_span: float
    user_cpu: float
    system_cpu: float
    memory_consumed: int  # Megabytes


@dataclass
class ResultException(Exception):
    """
    if a pytest exception occurred, hold info on that
    """
    excrepr: Exception


@dataclass
class ClientDied(Exception):
    """
    raise by a client when it dies or exits
    """
    pid: int
    host: str
    errored: bool = False
    message: Optional[str] = None


@dataclass
class AllClientsCompleted:
    """
    result placed in queue when all clients have completed
    """
    pass


@dataclass
class ResultTestStatus:
    """
    placed in results queue when test report is available
    """
    report: TestReport


class TestStateEnum(Enum):
    """
    possible states of a test
    """
    STARTED = 1
    FINISHED = 2
    RETRY = 3


@dataclass
class TestState:
    state: TestStateEnum
    host: str
    pid: int
    test_id: str
    test_batch: TestBatch


@dataclass
class ResultExit:
    worker_index: int
    test_count: int
    status: int
    duration: float
    resource_utilization: ResourceUtilization


@dataclass
class GroupTag:
    """
    holder for group information (batch of tests)
    """
    name: str
    priority: int = DEFAULT_PRIORITY
    restrict_to: TestExecutionConstraint = TestExecutionConstraint.SINGLE_PROCESS

    def __hash__(self):
        return self.name.__hash__()


@dataclass
class ReportStarted:
    nodeid: str
    location: str


@dataclass
class ReportFinished:
    nodeid: str
    location: str


ResultType = Union[TestState, ResultException, ResultExit, ResourceUtilization, ResultTestStatus]
