import os
import sys

verbose = False


def debug_print(msg: str, *args, **kwargs):
    global verbose
    if verbose:
        print(msg, *args, **kwargs)


def always_print(msg: str):
    msg = msg + '\n'
    os.write(sys.stderr.fileno(), msg.encode('utf-8'))