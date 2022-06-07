import multiprocessing
from multiprocessing.process import current_process
from pathlib import Path

import binascii
import inspect
import os
import secrets
import socket
import sys
from contextlib import closing
from typing import Union, Callable, Optional, Any, Dict

# to indicate if running as main (host) or not;  set in plugin.py
from dataclasses import dataclass

is_main = '--as-main' in sys.argv or '--remote-worker' in sys.argv
is_worker = '--as-worker' in sys.argv

_auth_key = None
_user_defined_port_alloc: Optional[Callable[[], int]] = None
_user_defined_auth: Optional[Callable[[], bytes]] = None


def set_user_defined_auth_tokn(func: Callable[[], bytes]):
    _user_defined_auth = func


def set_user_defined_port_alloc(func: Callable[[], int]):
    _user_defined_port_alloc = func


def get_auth_key() -> bytes:
    global _auth_key
    if hasattr(multiprocessing, 'parent_process') and multiprocessing.parent_process() is not None:
        _auth_key = current_process().authkey
    elif _auth_key is not None:
        return _auth_key
    elif _user_defined_auth:
        _auth_key = _user_defined_auth()
    elif 'AUTH_TOKEN_STDIN' in os.environ:
        auth_key = sys.stdin.readline().strip()
        _auth_key = binascii.a2b_hex(auth_key)
    else:
        _auth_key = secrets.token_bytes(64)
    current_process().authkey = _auth_key
    return _auth_key


def get_auth_key_hex():
    return binascii.b2a_hex(get_auth_key()).decode('utf-8')


class TestError(Exception):
    """
    To be thrown when a test setup/test environment issue occurs preventing test execution from even happening
    """
    pass


def priority(level: int):

    def decorator_priority(object):
        if inspect.isclass(object):
            for method in [getattr(object, m) for m in dir(object) if
                           inspect.isfunction(getattr(object, m)) and m.startswith('test')]:
                if not hasattr(method, "_pytest_priority"):
                    method._pytest_priority = level
        elif inspect.isfunction(object):
            object._pytest_priority = level
        else:
            raise Exception("group decorator can only decorate class or function object")
        return object

    return decorator_priority


def group(tag: Union["GroupTag", str]):
    """
    Decorator for grouping tests or a class of tests
    :param tag: unique name for group of tests to be serialized under execution
    """
    from pytest_mproc.data import GroupTag

    def decorator_group(object):
        if inspect.isclass(object):
            for method in [getattr(object, m) for m in dir(object) if inspect.isfunction(getattr(object, m)) and m.startswith('test')]:
                method._pytest_group = tag if isinstance(tag, GroupTag) else GroupTag(tag)
        elif inspect.isfunction(object):
            object._pytest_group = tag
        else:
            raise Exception("group decorator can only decorate class or function object")
        return object
    return decorator_group


def resource_utilization(time_span: float, start_rusage, end_rusage) -> "ResourceUtilization":
    from pytest_mproc.data import ResourceUtilization
    if time_span <= 0.001:
        return -1, -1, -1, end_rusage.ru_maxrss
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


def find_free_port():
    if _user_defined_port_alloc:
        return _user_defined_port_alloc()
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def fixture(fixturefunction, *, scope, **kargs):
    from _pytest.fixtures import fixture
    return fixture(fixturefunction, scope=scope, **kargs)


def get_ip_addr():
    hostname = socket.gethostname()
    # noinspection PyBroadException
    try:
        return socket.gethostbyname(hostname)
    except Exception:
        return None



DEFAULT_PRIORITY = 10
try:
    from pytest_mproc.data import GroupTag
except:
    # for orchestrationmanager to be able to come up independently
    # TODO: fix this
    pass


class FatalError(Exception):
    """
    raised to exit pytest immediately
    """


@dataclass
class Constants:
    """
    For storing global "constants"
    """
    ptmproc_args: Optional[Dict[str, Any]] = None

    @classmethod
    def set_ptmproc_args(cls, name: str, typ: Any):
        cls.ptmproc_args = cls.ptmproc_args or {}
        cls.ptmproc_args[name] = typ


@dataclass
class Settings:
    ssh_username: Optional[str] = os.environ.get("SSH_USERNAME")
    ssh_password: Optional[str] = None
    cache_dir: Optional[Path] = None
    tmp_root: Optional[Path] = None

    @classmethod
    def set_ssh_credentials(cls, username: str, password: Optional[str] = None):
        """
        Set SSH credentials

        :param username:  username for ssh (remote end username)
        :param password: optional password, if no password provided, sets password to None and assume passowrless
            login
        """
        cls.ssh_username = username
        cls.ssh_password = password

    @classmethod
    def set_cache_dir(cls, cache_dir: Optional[Path]):
        """
        Set persistent cache dir where cahced files will be stored

        :param cache_dir: path to use for files cached run to run
        """
        cls.cache_dir = Path(cache_dir) if cache_dir else None

    @classmethod
    def set_tmp_root(cls, tmp_dir: Optional[Path]):
        """
        Set root dir for tmp files on remote hosts

        :param tmp_dir:  root dir where temp directories are created (and removed on finalization)
        """
        cls.tmp_root = Path(tmp_dir) if tmp_dir else None
