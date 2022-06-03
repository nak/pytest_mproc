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
    # quiet = ['-q'] if user_output.verbose else []
    m = hashlib.sha256()

    hash_value = m.hexdigest()
    site_pkgs_dir = cache_dir / hash_value / "site-packages"
    #install_wheel = any([(Path(d) / "pytest_mproc").exists for d in project_config.src_paths])
    #if (site_pkgs_dir / "dataclasses.py").exists():
    #    import os
    #    os.remove(site_pkgs_dir/ "dataclasses.py")  # this is part of 3.7+ and causes problems if dep installs it
    if not site_pkgs_dir.exists():
        site_pkgs_dir.mkdir(parents=True)
    return site_pkgs_dir