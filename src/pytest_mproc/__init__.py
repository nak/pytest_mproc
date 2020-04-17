import inspect


def group(name):
    """
    Decorator for grouping tests or a class of tests
    :param name: unique name for group of tests to be serialized under execution
    """
    def decorator_group(object):
        if inspect.isclass(object):
            for method in [getattr(object, m) for m in dir(object) if inspect.isfunction(getattr(object, m)) and m.startswith('test')]:
                method._pytest_group = name
        elif inspect.isfunction(object):
            object._pytest_group = name
        else:
            raise Exception("group decorator can only decorate class or function object")
        return object
    return decorator_group
