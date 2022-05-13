import hashlib
import shutil
import subprocess
import sys

from pathlib import Path
from pytest_mproc import user_output
from pytest_mproc.ptmproc_data import ProjectConfig


def set_up_local(project_config: ProjectConfig, cache_dir: Path) -> Path:
    """
    install requirements to local cached dir to set up for creation of shiv

    :param project_config: proejct configuration defining what to install
    :param cache_dir: where to install requirements
    """
    quiet = ['-q'] if user_output.verbose else []
    m = hashlib.sha256()
    for requirements_path in project_config.pure_requirements_paths:
        with open(requirements_path, 'rb') as in_stream:
            for line in in_stream:
                m.update(line)
    hash_value = m.hexdigest()
    cache_dir = cache_dir / hash_value / "site-packages"
    install_wheel = any([(Path(d) / "pytest_mproc").exists for d in project_config.src_paths])
    if install_wheel:
        # mostly for running raw pytest_mproc code during unit/integration testing:
        root = Path(__file__).parent.parent.parent.parent
        setup = root / "setup.py"
        assert subprocess.run([sys.executable, setup, "bdist_wheel"],
                              cwd=str(root),
                              stdout=subprocess.DEVNULL).returncode == 0
        completed = subprocess.run([sys.executable, '-m', 'pip', "install", "--target", str(cache_dir), "--upgrade",
                                    root / "dist" / "pytest_mproc-4.2.0-py3-none-any.whl"])
        if completed.returncode != 0:
            raise SyntaxError("Failed ot install wheel for pytest_mproc")
    if not cache_dir.exists():
        try:
            cache_dir.mkdir(parents=True)
            for requirements_path in project_config.pure_requirements_paths:
                completed = subprocess.run([sys.executable, '-m', 'pip', "install", "--target", str(cache_dir),
                                            "--force-reinstall",
                                            "-r", str(requirements_path)] + quiet,
                                           stderr=sys.stderr)
                if completed.returncode != 0:
                    raise SyntaxError(f"Failed to install {str(requirements_path)}")
            completed = subprocess.run([sys.executable, '-m', 'pip', "install", "--target", str(cache_dir),
                                        "--force-reinstall",
                                        "pytest"] + quiet,
                                       stderr=sys.stderr)
            if completed.returncode != 0:
                raise SyntaxError("Failed to install pytest")
            if not any([(Path(d) / "pytest_mproc").exists for d in project_config.src_paths]):
                completed = subprocess.run([sys.executable, '-m', 'pip', "install", "--target", str(cache_dir),
                                            "--force-reinstall",
                                             "pytest_mproc"] + quiet, )
                if completed.returncode != 0:
                    raise SyntaxError("Failed to install pytest_mproc")
        except Exception:
            shutil.rmtree(cache_dir)
            raise
    return cache_dir