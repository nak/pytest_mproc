import inspect
import sys


def group(name, priority:int = 0):
    """
    Decorator for grouping tests or a class of tests
    :param name: unique name for group of tests to be serialized under execution
    """
    def decorator_group(object):
        if inspect.isclass(object):
            for method in [getattr(object, m) for m in dir(object) if inspect.isfunction(getattr(object, m)) and m.startswith('test')]:
                method._pytest_group = (name, priority)
        elif inspect.isfunction(object):
            object._pytest_group = (name, priority)
        else:
            raise Exception("group decorator can only decorate class or function object")
        return object
    return decorator_group


def resource_utilization(time_span: float, start_rusage, end_rusage):
    if time_span <= 0.001:
        return -1, -1, end_rusage.ru_maxrss
    if sys.platform.lower() == 'darwin':
        # OS X is in bytes
        delta_mem = (end_rusage.ru_maxrss - start_rusage.ru_maxrss) / 1000.0
    else:
        delta_mem = (end_rusage.ru_maxrss - start_rusage.ru_maxrss)
    return ((end_rusage.ru_utime - start_rusage.ru_utime) / time_span) * 100.0, \
           ((end_rusage.ru_stime - start_rusage.ru_stime) / time_span) * 100.0, \
           delta_mem
