import hashlib
import shutil
import subprocess
import sys

from pathlib import Path
from pytest_mproc import user_output
from pytest_mproc.ptmproc_data import ProjectConfig
# from pytest_mproc.user_output import debug_print


def set_up_local(project_config: ProjectConfig, cache_dir: Path) -> Path:
    """
    install requirements to local cached dir to set up for creation of shiv

    :param project_config: proejct configuration defining what to install
    :param cache_dir: where to install requirements
    """
    m = hashlib.sha256()
    hash_value = m.hexdigest()
    site_pkgs_dir = cache_dir / hash_value / "site-packages"
    return site_pkgs_dir