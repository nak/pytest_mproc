import asyncio
import multiprocessing
import inspect
import socket
import sys

from contextlib import closing
from dataclasses import dataclass
from queue import Empty, Full
from typing import Union, Callable, Optional, Any, Dict

from pytest_mproc.data import GroupTag

is_main = '--as-main' in sys.argv or '--remote-worker' in sys.argv
is_worker = '--as-worker' in sys.argv or 'pytest_mproc.worker' in sys.argv

_user_defined_port_alloc: Optional[Callable[[], int]] = None
_user_defined_auth: Optional[Callable[[], bytes]] = None


def set_user_defined_auth_token(func: Callable[[], bytes]):
    global _user_defined_auth
    _user_defined_auth = func


def set_user_defined_port_alloc(func: Callable[[], int]):
    global _user_defined_port_alloc
    _user_defined_port_alloc = func


def priority(level: int):
    """
    decorator for setting test priority (lower number is higher priority)
    """

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


def _find_free_port():
    if _user_defined_port_alloc:
        return _user_defined_port_alloc()
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# noinspection SpellCheckingInspection
def fixture(fixturefunction, *, scope, **kargs):
    from _pytest.fixtures import fixture as pt_fixture
    return pt_fixture(fixturefunction, scope=scope, **kargs)


def _get_my_ip() -> str:
    hostname = socket.gethostname()
    # noinspection PyBroadException
    try:
        return socket.gethostbyname(hostname)
    except Exception:
        return "<<unknown ip address>>"


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

    async def get(self, immediate: bool = False):
        """
        :return: next item in queue.
        :param immediate: If False, will block; use asyncio.wait_for to timeout a call to this method.
           If True, return immediately if item ia available, else return None
        """
        while True:
            try:
                item = self._q.get(block=False)
                self._q.task_done()
                return item
            except Empty:
                if immediate:
                    return None
                await asyncio.sleep(self.INTERVAL_POLLING)

    async def wait(self):
        """
        wait until all results are out of queue.  Called once all clients are done putting results to flush and
        process remaining items
        THIS WILL WAIT 20 seconds for remaining results to come in (presumable after all worker threads exited),
        anything longer than that is deemed as no more results to process.  Either that there is a severe network
        delay and we have bigger issues
        """
        while True:
            try:
                await asyncio.wait_for(self.get(), timeout=20)
            except asyncio.TimeoutError:
                break

    async def put(self, item, immediate: bool = False) -> bool:
        """
        put an item in the queue

        :param item: item to place
        :param immediate: return immediately indicating whether put was successful if set True; otherwise blocks;
           use asyncio.wait_for to utilize a timeout on this method
        """
        while True:
            try:
                self._q.put(item, block=False)
                await asyncio.sleep(0)
                return True
            except Full:
                if immediate:
                    return False
                await asyncio.sleep(self.INTERVAL_POLLING)

    def close(self):
        self._q.close()

    def get_nowait(self):
        return self.get(immediate=True)

    def put_nowait(self, item):
        return self.put(item, immediate=True)

    def join(self, timeout: float = TIMEOUT_JOIN) -> None:
        # assume join only called on "put" thread when it expects
        # all items in the queue to be drained and no more puts are expected
        self._q.close()
