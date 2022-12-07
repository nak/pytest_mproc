"""
Convenience definitions to work around the faults and silliness underlying multiprocessing module in places
"""
import multiprocessing
from multiprocessing.managers import SyncManager
from typing import Any, Dict, TypeVar, Generic, Optional

from pytest_mproc.user_output import always_print


class SafeSerializable:

    def __getstate__(self):
        raise(Exception)
        always_print(f">>>>>>>>>>>>>>>>>>> PICKLING {self.__dict__}")
        d = dict(self.__dict__)
        for key in [k for k in d if k.startswith('_')]:
            del d[key]
        return d

    def __setstate__(self, d: Dict[str, Any]):
        raise Exception()
        always_print(f">>>>>>>>>>>>>>>>>>>> UPDATING STATE {d}  {self.__dict__}")
        self.__dict__.update(d)


T = TypeVar('T')


class JoinableQueue(Generic[T], SafeSerializable):

    def __init__(self):
        self._q = multiprocessing.JoinableQueue()

    def get(self, block: bool = True, timeout: Optional[float] = None) -> T:
        if not hasattr(self, '_q'):
            raise RuntimeError("To use JoinableQueue in distributed environment, it must be created through "
                               "multiprocessing.SyncManager")
        return self._q.get(block=block, timeout=timeout)

    def put(self, obj: T, block: bool = True, timeout: Optional[float] = None):
        if not hasattr(self, '_q'):
            raise RuntimeError("To use JoinableQueue in distributed environment, it must be created through "
                               "multiprocessing.SyncManager")
        return self._q.put(obj, block=block, timeout=timeout)

    def task_done(self):
        if not hasattr(self, '_q'):
            raise RuntimeError("To use JoinableQueue in distributed environment, it must be created through "
                               "multiprocessing.SyncManager")
        return self._q.task_done()

    def close(self):
        return self._q.close()

    def join(self):
        if not hasattr(self, '_q'):
            raise RuntimeError("To use JoinableQueue in distributed environment, it must be created through "
                               "multiprocessing.SyncManager")
        return self._q.join()


mgr = None


def SharedJoinableQueue():
    global mgr
    if mgr is None:
        SyncManager.register("JoinableQueue", JoinableQueue, exposed=[
            'get', 'put', 'task_done', 'join', 'close'
        ])
        mgr = SyncManager()
        mgr.start()
    return mgr.JoinableQueue()

