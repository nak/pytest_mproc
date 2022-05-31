from dataclasses import dataclass
from enum import Enum
from typing import List, Union, Optional

from pytest_mproc import DEFAULT_PRIORITY
from _pytest._code.code import ReprExceptionInfo
from _pytest.reports import TestReport


class TestExecutionConstraint(Enum):
    SINGLE_PROCESS ="SINGLE_PROCESS"
    SINGLE_NODE = "SINGLE_NODE"


@dataclass
class TestBatch:
    test_ids: List[str]
    priority: int = DEFAULT_PRIORITY
    restriction: TestExecutionConstraint = TestExecutionConstraint.SINGLE_PROCESS


@dataclass
class ResourceUtilization:
    time_span: float
    user_cpu: float
    system_cpu: float
    memory_consumed: int  # Megabytes


@dataclass
class ResultException(Exception):
    excrepr: ReprExceptionInfo


@dataclass
class ClientDied(Exception):
    pid: int
    host: str
    errored: bool = False
    message: Optional[str] = None


@dataclass
class AllClientsCompleted:
    hosts: List[str]


@dataclass
class ResultTestStatus:
    report: TestReport


class TestStateEnum(Enum):
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
    name: str
    priority: int = DEFAULT_PRIORITY
    restrict_to: TestExecutionConstraint = TestExecutionConstraint.SINGLE_PROCESS

    def __hash__(self):
        return self.name.__hash__()


ResultType = Union[TestState, ResultException, ResultExit, ResourceUtilization, ResultTestStatus]
