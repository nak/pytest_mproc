verbose = False


def debug_print(msg: str, *args, **kwargs):
    global verbose
    if verbose:
        print(msg, *args, **kwargs)