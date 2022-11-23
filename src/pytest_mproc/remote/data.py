import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


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
