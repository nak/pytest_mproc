import os
import sys

from pytest_mproc import get_ip_addr

verbose = bool(os.environ.get("PTMPROC_VERBOSE", False) not in ['0', 'False', 'false', 'FALSE'])


def set_verbose(is_verbose: bool):
    """
    set verbosity of output

    :param is_verbose: if True, prints debug statements on actions taken
    """
    global verbose
    verbose = bool(is_verbose)


def debug_print(msg: str, *args):
    """
    Print only if verbose is True.  Is is recommended to use format strings just as in logging.* statements
    to avoid unnecessary pre-computation when a message is not printed (verbosity off)

    :param msg: string or format string
    :param args: args for format string if required
    """
    global verbose
    if verbose:
        msg = f"[{get_ip_addr()}] {msg}"
        print(msg % args)


def always_print(msg: str, *args, as_error: bool = False):
    """
    Always print a message (to stderr)

    :param msg: a string or format string
    :param args: args to go with format string if required
    :return:
    """
    msg = (msg % args) + '\n'
    if verbose:
        msg = f"[{get_ip_addr()}] {msg}"
    os.write(sys.stderr.fileno() if as_error else sys.stdout.fileno(), msg.encode('utf-8'))