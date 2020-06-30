from dataclasses import dataclass
from enum import Enum
from typing import List, Any

from _pytest._code.code import ReprExceptionInfo
from _pytest.reports import TestReport


class TestExecutionRestriction(Enum):
    SINGLE_PROCESS ="SINGLE_PROCESS"
    SINGLE_NODE = "SINGLE_NODE"


@dataclass
class TestBatch:
    test_ids: List[str]
    restriction: TestExecutionRestriction = TestExecutionRestriction.SINGLE_NODE


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
