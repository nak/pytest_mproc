import binascii
import multiprocessing
import os
import secrets
import sys
from configparser import ConfigParser
from multiprocessing import current_process
from pathlib import Path
from typing import Optional

from pytest_mproc import _user_defined_auth


DEFAULT_SYS_PATH=Path("/etc/pytest_mproc.ini")


def ini_path() -> Optional[Path]:
    """
    return location of a defined ini file or None if not specified
    """
    if 'SYSTEM_CONFIG_INI' in os.environ:
        path = Path(os.environ['SYSTEM_CONFIG_INI'])
        if not path.is_file():
            raise ValueError("Env var SYSTEM_CONFIG_INI does not point to a valid ini file")
    elif DEFAULT_SYS_PATH.is_file():
        return DEFAULT_SYS_PATH
    else:
        path = None
    return path


def is_local_auth() -> bool:
    return not _user_defined_auth and 'AUTH_TOKEN_STDIN' not in os.environ and ini_path() is None


def get_auth_key() -> bytes:
    global _auth_key
    if hasattr(multiprocessing, 'parent_process') and multiprocessing.parent_process() is not None:
        _auth_key = current_process().authkey
    elif _auth_key is not None:
        return _auth_key
    elif _user_defined_auth:
        _auth_key = _user_defined_auth()
    elif os.environ.get("AUTH_TOKEN_STDIN", None) == '1':
        auth_key = sys.stdin.readline().strip()
        _auth_key = binascii.a2b_hex(auth_key)
    elif ini_path() is not None and ini_path().is_file():
        parser = ConfigParser()
        parser.read(ini_path())
        auth_key = parser.get(section='pytest_mproc', option='auth_token', fallback=None)
        if auth_key is None:
            raise ValueError(f"No 'auth_token' provided in {ini_path}")
        _auth_key = binascii.a2b_hex(auth_key)
    else:
        _auth_key = secrets.token_bytes(64)
    current_process().authkey = _auth_key
    return _auth_key


_auth_key = None


def get_auth_key_hex():
    return binascii.b2a_hex(get_auth_key()).decode('utf-8')
