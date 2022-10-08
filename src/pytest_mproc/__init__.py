import asyncio
import multiprocessing
import time
import binascii
import inspect
import os
import secrets
import socket
import sys

from contextlib import closing
from dataclasses import dataclass
from multiprocessing.process import current_process
from pathlib import Path
from queue import Empty, Full
from typing import Union, Callable, Optional, Any, Dict

from pytest_mproc.data import GroupTag

is_main = '--as-main' in sys.argv or '--remote-worker' in sys.argv
is_worker = '--as-worker' in sys.argv or 'pytest_mproc.worker' in sys.argv

_auth_key = None
_user_defined_port_alloc: Optional[Callable[[], int]] = None
_user_defined_auth: Optional[Callable[[], bytes]] = None


def set_user_defined_auth_token(func: Callable[[], bytes]):
    global _user_defined_auth
    _user_defined_auth = func


def set_user_defined_port_alloc(func: Callable[[], int]):
    global _user_defined_port_alloc
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

    def decorator_priority(obj_):
        if inspect.isclass(obj_):
            for method in [getattr(obj_, m) for m in dir(obj_) if
                           inspect.isfunction(getattr(obj_, m)) and m.startswith('test')]:
                if not hasattr(method, "_pytest_priority"):
                    method._pytest_priority = level
        elif inspect.isfunction(obj_):
            obj_._pytest_priority = level
        else:
            raise Exception("group decorator can only decorate class or function object")
        return obj_

    return decorator_priority


def group(tag: Union["GroupTag", str]):
    """
    Decorator for grouping tests or a class of tests
    :param tag: unique name for group of tests to be serialized under execution
    """
    from pytest_mproc.data import GroupTag

    def decorator_group(obj):
        if inspect.isclass(obj):
            for method in [getattr(obj, m) for m in dir(obj)
                           if inspect.isfunction(getattr(obj, m)) and m.startswith('test')]:
                method._pytest_group = GroupTag(tag) if isinstance(tag, str) else tag
        elif inspect.isfunction(obj):
            obj._pytest_group = tag
        else:
            raise Exception("group decorator can only decorate class or function object")
        return obj
    return decorator_group


def find_free_port():
    if _user_defined_port_alloc:
        return _user_defined_port_alloc()
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# noinspection SpellCheckingInspection
def fixture(fixturefunction, *, scope, **kargs):
    from _pytest.fixtures import fixture
    return fixture(fixturefunction, scope=scope, **kargs)


def get_ip_addr() -> str:
    hostname = socket.gethostname()
    # noinspection PyBroadException
    try:
        return socket.gethostbyname(hostname)
    except Exception:
        return "<<unknown ip address>>"


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
        :param password: optional password, if no password provided, sets password to None and assume password-less
            login
        """
        cls.ssh_username = username
        cls.ssh_password = password

    @classmethod
    def set_cache_dir(cls, cache_dir: Optional[Path]):
        """
        Set persistent cache dir where cached files will be stored

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


class AsyncMPQueue:
    """
    wraps mp Queue to provide an async (albeit polling) interface
    """

    INTERVAL_POLLING = 0.5  # seconds
    TIMEOUT_JOIN = 5.0  # seconds

    def __init__(self, wrapped_q: multiprocessing.JoinableQueue):
        assert type(wrapped_q) != AsyncMPQueue
        self._q = wrapped_q

    def raw(self) -> multiprocessing.JoinableQueue:
        return self._q

    async def get(self, max_tries: Optional[int] = None):
        """
        :return: next item in queue
        """
        count = 0
        while max_tries is None or count < max_tries:
            try:
                item = self._q.get(block=False)
                self._q.task_done()
                return item
            except Empty:
                count += 1
                await asyncio.sleep(self.INTERVAL_POLLING)

    async def put(self, item, max_tries: Optional[float] = None) -> bool:
        """
        put an item in the queue

        :param item: item to place
        :param max_tries: maximum number of tries before returning False, or None for no restriction on tries
        """
        count = 0
        while max_tries is None or count < max_tries:
            try:
                self._q.put(item, block=False)
                await asyncio.sleep(0)
                return True
            except Full:
                count += 1
                await asyncio.sleep(self.INTERVAL_POLLING)
        return False

    def close(self):
        self._q.close()

    def get_nowait(self):
        return self._q.get_nowait()

    def put_nowait(self, item):
        return self._q.put(item)

    async def join(self, timeout: float = TIMEOUT_JOIN) -> None:
        # assume join only called on "put" thread when it expects
        # all items in the queue to be drained and no more puts are expected
        start = time.monotonic()
        while not self._q.empty():
            await asyncio.sleep(self.INTERVAL_POLLING)
            if time.monotonic() - start > timeout:
                raise TimeoutError(f"Timed out after {timeout:.1f} seconds joining queue")
