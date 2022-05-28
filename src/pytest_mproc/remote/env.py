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
    quiet = ['-q'] if user_output.verbose else []
    m = hashlib.sha256()
    hash_value = m.hexdigest()
    site_pkgs_dir = cache_dir / hash_value / "site-packages"
    install_wheel = any([(Path(d) / "pytest_mproc").exists for d in project_config.src_paths])
    if False and install_wheel:
        # mostly for running raw pytest_mproc code during unit/integration testing:
        root = Path(__file__).parent.parent.parent.parent
        setup = root / "setup.py"
        if (root / "dist").exists():
            shutil.rmtree(root / "dist")
        assert subprocess.run([sys.executable, str(setup), "sdist"],
                              cwd=str(root),
                              stdout=subprocess.DEVNULL).returncode == 0
        sdist = next((root / "dist").glob('*.tar.gz'))
        completed = subprocess.run([sys.executable, '-m', 'pip', "install", "--target", str(site_pkgs_dir), "--upgrade",
                                    sdist],
                                   cwd=root)
        if completed.returncode != 0:
            raise SyntaxError("Failed ot install wheel for pytest_mproc")
    if (site_pkgs_dir / "dataclasses.py").exists():
        import os
        os.remove(site_pkgs_dir/ "dataclasses.py")  # this is part of 3.7+ and causes problems if dep installs it
    if not site_pkgs_dir.exists():
        try:
            site_pkgs_dir.mkdir(parents=True)
            completed = subprocess.run([sys.executable, '-m', 'pip', "install", "--target", str(site_pkgs_dir),
                                        "--force-reinstall",
                                        "pytest"] + quiet,
                                       stderr=sys.stderr)
            if completed.returncode != 0:
                raise SyntaxError("Failed to install pytest")
            if not any([(Path(d) / "pytest_mproc").exists for d in project_config.src_paths]):
                completed = subprocess.run([sys.executable, '-m', 'pip', "install", "--target", str(site_pkgs_dir),
                                            "--force-reinstall",
                                             "pytest_mproc"] + quiet, )
                if completed.returncode != 0:
                    raise SyntaxError("Failed to install pytest_mproc")
        except Exception:
            shutil.rmtree(site_pkgs_dir)
            raise
    return site_pkgs_dir