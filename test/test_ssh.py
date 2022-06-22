import pytest

from pytest_mproc.remote.ssh import SSHClient

from pytest_mproc import find_free_port


@pytest.mark.asyncio
async def test_find_free_port():
    ssh_client = SSHClient(host='localhost', port=find_free_port(), username=None)
    port = await ssh_client.find_free_port()
    assert port > 0

