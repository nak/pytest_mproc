"""
Common data structures definitions
"""
import sys
from dataclasses import dataclass
from enum import Enum
from typing import List, Union, Optional

from pytest_mproc.constants import DEFAULT_PRIORITY

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
class WorkerStarted:
    worker_id: str
    pid: int
    host: str


@dataclass
class WorkerExited:
    """
    raise by a client when it dies or exits
    """
    worker_id: str
    pid: int
    host: str
    errored: bool = False
    message: Optional[str] = None


@dataclass
class AllWorkersDone:
    pass


class ClientException(Exception):
    """
    raised when client dies with exception
    """
    worker_id: str
    exc: Exception


@dataclass
class WorkerStatus:
    session_id: str
    worker_id: str


class TestStateEnum(Enum):
    """
    possible states of a test
    """
    STARTED = 1
    FINISHED = 2
    RETRY = 3


@dataclass
class StatusTestState:
    state: TestStateEnum
    host: str
    pid: int
    test_id: str
    test_batch: TestBatch


@dataclass
class StatusTestsComplete(WorkerStatus):
    test_count: int
    status: int
    duration: float
    resource_utilization: ResourceUtilization


StatusType = Union[StatusTestState, Exception, StatusTestsComplete]


@dataclass
class ReportActivity:
    """
    base class for report state changes (started/finished)
    """


@dataclass
class ReportStarted(ReportActivity):
    nodeid: str
    location: str


@dataclass
class ReportFinished(ReportActivity):
    nodeid: str
    location: str


ReportUpdate = Union[ReportActivity, TestReport]


def resource_utilization(time_span: float, start_rusage, end_rusage) -> "ResourceUtilization":
    if time_span <= 0.001:
        return ResourceUtilization(-1, -1, -1, end_rusage.ru_maxrss)
    if sys.platform.lower() == 'darwin':
        # OS X is in bytes
        delta_mem = (end_rusage.ru_maxrss - start_rusage.ru_maxrss) / 1000.0
    else:
        delta_mem = (end_rusage.ru_maxrss - start_rusage.ru_maxrss)
    ucpu_secs = end_rusage.ru_utime - start_rusage.ru_utime
    scpu_secs = end_rusage.ru_stime - start_rusage.ru_stime
    return ResourceUtilization(
        time_span,
        (ucpu_secs / time_span) * 100.0,
        (scpu_secs / time_span) * 100.0,
        delta_mem)
