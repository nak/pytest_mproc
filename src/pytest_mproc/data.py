from dataclasses import dataclass
from enum import Enum
from typing import List, Any, Set, Dict

from _pytest._code.code import ReprExceptionInfo
from _pytest.reports import TestReport


DEFAULT_PRIORITY = 10


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
class ResultException:
    excrepr: ReprExceptionInfo


@dataclass
class ResultTestStatus:
    report: TestReport


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
