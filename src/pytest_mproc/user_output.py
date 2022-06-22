import os
import sys

from pytest_mproc import get_ip_addr

is_verbose = os.environ.get("PTMPROC_VERBOSE", False) not in ['0', 'False', 'false', 'FALSE', False]


def set_verbose(verbose: bool):
    """
    set verbosity of output

    :param verbose: if True, prints debug statements on actions taken
    """
    global is_verbose
    is_verbose = bool(verbose)


def debug_print(msg: str, *args):
    """
    Print only if verbose is True.  It is recommended to use format strings just as in logging.* statements
    to avoid unnecessary pre-computation when a message is not printed (verbosity off)

    :param msg: string or format string
    :param args: args for format string if required
    """
    global is_verbose
    if is_verbose:
        msg = f"[{get_ip_addr()}] {msg}"
        print(msg % args)


def always_print(msg: str, *args, as_error: bool = False):
    """
    Always print a message (to stderr)

    :param msg: a string or format string
    :param args: args to go with format string if required
    :param as_error: whether to put to stdout or stderr
    :return:
    """
    if args:
        msg = (msg % args) + '\n'
    else:
        msg += '\n'
    if is_verbose:
        msg = f"[{get_ip_addr()}] {msg}"
    os.write(sys.stderr.fileno() if as_error else sys.stdout.fileno(), msg.encode('utf-8'))
