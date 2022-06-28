import asyncio
import os
import shutil
import subprocess
import sys
from contextlib import suppress
from glob import glob
from pathlib import Path
from queue import Empty
from typing import Any
from unittest.mock import MagicMock

import pytest

from pytest_mproc import user_output
from pytest_mproc.data import AllClientsCompleted, ClientDied, TestBatch, ResultExit
from pytest_mproc.main import Orchestrator
from pytest_mproc.remote_sessions import RemoteSessionManager
from pytest_mproc.orchestration import OrchestrationManager
from pytest_mproc.ptmproc_data import ProjectConfig, RemoteWorkerConfig
from pytest_mproc.user_output import always_print


@pytest.fixture()
def mgr():
    mgr = OrchestrationManager.create_server(adderss=('localhost', 8734))
    yield mgr
    mgr.shutdown()


@pytest.fixture()
def project_config(tmp_path):
    yield ProjectConfig(project_name='test',
                        project_root=tmp_path,
                        test_files=[],
                        )


@pytest.mark.asyncio
async def test_validate_clients(project_config, mgr):
    user_output.verbose = True
    proc1 = await asyncio.subprocess.create_subprocess_exec("yes", stdout=asyncio.subprocess.DEVNULL,
                                                            stderr=asyncio.subprocess.DEVNULL)
    proc2 = await asyncio.subprocess.create_subprocess_exec("yes", stdout=asyncio.subprocess.DEVNULL,
                                                            stderr=asyncio.subprocess.DEVNULL)
    try:
        result_q = mgr.get_results_queue()
        remote_session = RemoteSessionManager(remote_sys_executable=None,
                                              project_config=project_config,
                                              mgr=mgr)
        count = 0

        def mock_run(args, stdout: Any, stdin: Any=None, stderr: Any= None, timeout: int=0, **_kwargs):
            nonlocal count
            count += 1
            if count >= 3 and 'localhost' in args:
                return subprocess.CompletedProcess(returncode=1, args=())
            elif count > 6:
                assert 'localhost' not in args
                return subprocess.CompletedProcess(returncode=1, args=())
            return subprocess.CompletedProcess(returncode=0, args=())

        subprocess.run = MagicMock(side_effect=mock_run)
        await asyncio.wait_for(
            remote_session.validate_clients(results_q=result_q,
                                            remote_host_procs={'localhost': proc1,
                                                               '127.0.0.1': proc2}),
            timeout=20
        )
        assert proc1.returncode == -15
        assert proc2.returncode == -15
    finally:
        with suppress(Exception):
            proc1.kill()
        with suppress(Exception):
            proc2.kill()


@pytest.mark.asyncio
async def test_start_workers(mgr: OrchestrationManager, project_config: ProjectConfig):
    root = Path(__file__).parent.parent / 'src'
    files = glob(str(root / 'pytest_mproc' / '*.py')) + glob(str(root / 'pytest_mproc' / 'remote' / '*.py'))
    project_config.test_files = [Path(f).relative_to(root) for f in files] + \
        [Path('test_one.py')]
    os.mkdir(project_config.project_root / 'pytest_mproc')
    os.mkdir(project_config.project_root / 'pytest_mproc' / 'remote')
    for f in files:
        shutil.copy(f, project_config.project_root / Path(f).relative_to(root))
    with open(project_config.project_root / 'test_one.py', 'w') as out_stream:
        out_stream.write("""
def test_one():
    pass

""")
    user_output.verbose = False
    host_q = asyncio.Queue(10)
    remote_session = RemoteSessionManager(remote_sys_executable=sys.executable,
                                          project_config=project_config)
    cwd = os.getcwd()
    test_q = mgr.get_test_queue()
    await test_q.put(None)
    mgr.signal_all_tests_sent()
    argv = sys.argv
    assert project_config.project_root.exists()
    task = None
    try:
        sys.argv = ['-s', 'test_one.py']
        os.chdir(project_config.project_root)
        await host_q.put(RemoteWorkerConfig(remote_host='localhost', arguments={}))
        await host_q.put(None)

        result_q = OrchestrationManager.create_client(address=('localhost', 8734)).get_results_queue()

        async def go():
            try:
                async with remote_session.start_workers(server='localhost', server_port=8734,
                                                        hosts_q=host_q, timeout=10, deploy_timeout=10,
                                                        ) as port:
                    assert port == (-1, -1)  # no delegate set
            except Empty as e:
                await result_q.put(e)
                mgr.shutdown()
                pytest.fail(f"Exception in start workers {e}")
        task = asyncio.create_task(go())
        r = await asyncio.wait_for(result_q.get(), timeout=20)
        while True:
            assert type(r) in (ClientDied, AllClientsCompleted, ResultExit)
            if type(r) == AllClientsCompleted:
                try:
                    r = await asyncio.wait_for(result_q.get(), timeout=5)
                    assert False, f"Result q not empty as expected: {r}"
                except asyncio.TimeoutError:
                    pass
                break
            r = await asyncio.wait_for(result_q.get(), timeout=20)
        remote_session.shutdown()
        await asyncio.wait_for(task, timeout=3)
    finally:
        sys.argv = argv
        os.chdir(cwd)
        if task:
            with suppress(Exception):
                task.cancel()


@pytest.mark.asyncio
async def test_populate_test_queue(project_config: ProjectConfig):
    port = 9341
    async with Orchestrator(uri=f"localhost:{port}", project_config=project_config) as orchestrator:
        test_q = orchestrator._test_q
        # orchestrator._test_q = orchestrator.mp_mgr.get_test_queue()
        tests = [TestBatch(['test1.1', 'test1.2'], priority=2)] + \
                [TestBatch([f'test3.{n}' for n in range(100)], priority=5)] + \
                [TestBatch([f'test{n}.1'], priority=1) for n in range(100)]
        try:
            task = asyncio.create_task(orchestrator.populate_test_queue(tests=tests))
            test_batch = await asyncio.wait_for(orchestrator._test_q.get(), timeout=5)
            index = 0
            assert test_batch.test_ids == ['test1.1', 'test1.2']
            while test_batch:
                test_batch = await asyncio.wait_for(test_q.get(), timeout=5)
                index += 1
                if index == 1:
                    assert test_batch.test_ids == [f'test3.{n}' for n in range(100)]
                elif index > 1 and test_batch is not None:
                    assert test_batch.test_ids == [f'test{index - 2}.1']
            try:
                batch = test_q.get_nowait()
                assert False, f"Additional test found: {batch}"
            except Empty:
                pass
        finally:
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize('uri', [
    "localhost:9342",
    "delegated://pi@",
])
async def test_start_remote(project_config: ProjectConfig, uri: str, request):
    project_config.project_root = Path(__file__).parent.parent
    project_config.test_files += [Path('test') / 'test_mproc_runs.py',
                                  Path('testsrc') / 'testcode' / '*.py',
                                  Path('testsrc') / 'testcode' / '**' / '*.py']
    orchestrator = Orchestrator(uri=uri, project_config=project_config)
    cwd = os.getcwd()
    argv = sys.argv
    env = {'PYTHONPATH': f"{str(Path(__file__).parent.parent / 'src')}:{str(Path(__file__).parent.parent / 'testsrc')}"}
    async with orchestrator:
        try:
            sys.argv = [sys.argv[0]] + ['test/test_mproc_runs.py', '-s', '--cores', '1']
            assert project_config.project_root.exists()
            os.chdir(project_config.project_root)
            await orchestrator.start_remote(
                remote_workers_config=[
                    RemoteWorkerConfig(remote_host='localhost', remote_root=project_config.project_root),
                    RemoteWorkerConfig(remote_host='127.0.0.1', remote_root=project_config.project_root)
                ],
                deploy_timeout=20,
                env=env)
            items = [TestBatch(test_ids=['test_mproc_runs.py::test_some_alg1'])]
            mgr = OrchestrationManager.create_client(address=('localhost', orchestrator.port))
            test_q = mgr.get_test_queue()
            result_q = mgr.get_results_queue()
            for item in items:
                await asyncio.wait_for(test_q.put(item), timeout=5)
            mgr.signal_all_tests_sent()
            await asyncio.wait_for(test_q.put(None), timeout=5)
            await asyncio.wait_for(test_q.put(None), timeout=5)
            result = await asyncio.wait_for(result_q.get(), timeout=10)
            while not isinstance(result, AllClientsCompleted):
                result = await asyncio.wait_for(result_q.get(), timeout=100)
            try:
                await asyncio.wait_for(result_q.get(), timeout=2)
                assert False, "unexpected result after signalled that all client completed"
            except asyncio.TimeoutError:
                pass
        finally:
            os.chdir(cwd)
            sys.argv = argv
