from dataclasses import dataclass
from enum import Enum

from pytest_mproc import find_free_port


class RoleEnum(Enum):
    MASTER = "MASTER"
    COORDINATOR = "COORDINATOR"
    WORKER = "WORKER"


@dataclass
class MPManagerConfig:
    role: RoleEnum
    num_processes: int
    global_mgr_host: str
    global_mgr_port: int
    node_mgr_host: str = '127.0.0.1'
    node_mgr_port: int = find_free_port()


