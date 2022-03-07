import logging

import pytest
import asyncio
import subprocess

from pathlib import Path
try:
    from pytest_mproc.remote.bundle import Bundle
    have_shiv = True
except ImportError:
    # probably client and don't have shiv
    have_shiv = False

_base_dir = Path(__file__).parent.parent
_logger = logging.getLogger(__file__)
ssh_pswdless = (Path.home() / ".ssh" / "authorized_keys").exists()


@pytest.fixture(scope='session')
def bundle(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp("remote_testing")
    upload_tmpdir = tmpdir_factory.mktemp("uploads")
    with Bundle.create(
            root_dir=Path(str(tmpdir)),
            src_dirs=[_base_dir / "src", _base_dir / "testsrc"],
            requirements_path=_base_dir / "requirements.txt",
            tests_dir=_base_dir / "test",
            remote_root=Path(upload_tmpdir),
    ) as bundle:
        bundle.add_file(_base_dir / "test" / "run_test.sh", "scripts/run_test.sh")
        yield bundle
        asyncio.get_event_loop().run_until_complete(bundle.cleanup())


@pytest.mark.skipif(not have_shiv, reason="shiv not present for testing")
def test_create_bundle(bundle):
    proc = subprocess.run([bundle.shiv_path, ".", "-k", "alg1"], cwd=Path(__file__).parent)
    assert proc.returncode == 0


@pytest.mark.skipif(not ssh_pswdless, reason="No permission to ssh to localhost without password input")
@pytest.mark.skipif(not have_shiv, reason="shiv not present for testing")
@pytest.mark.asyncio
async def test_execute_bundle(bundle):
    lines = []
    try:
        async for line in bundle.execute_remote("localhost", ".", "-k", "alg1"):
            lines.append(line)
            print(line)
    except SystemError:
        text = '\n'.join(lines)
        assert False, f"Execution failed:\n{text}"
