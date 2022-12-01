import multiprocessing
import os
from dataclasses import dataclass
from multiprocessing import JoinableQueue
from pathlib import Path
from typing import List

from _pytest.reports import TestReport

from pytest_mproc import _find_free_port
from pytest_mproc.data import TestBatch, ResultTestStatus, TestState, TestStateEnum
from pytest_mproc.fixtures import Global
from pytest_mproc.worker import WorkerSession, WorkerAgent

WORKER_TEST_DIR = Path(__file__).parent / "resources" / "project_tests"


def test__put():
    test_q = JoinableQueue()
    results_q = JoinableQueue()
    session = WorkerSession(worker_id="Worker-1-1", test_q=test_q, results_q=results_q)
    n = 1
    for _ in range(4):
        session._put(n)
        assert results_q.empty()
        n += 1
    session._put(n)
    assert results_q.get() == [m + 1 for m in range(5)]


def test_test_loop():
    test_q = JoinableQueue(10)
    results_q = JoinableQueue(10)
    session = WorkerSession(worker_id="Worker-1-1", test_q=test_q, results_q=results_q)
    items_ran = {}

    def mock_test_q_populate(test_q: JoinableQueue):
        for m in range(100):
            batch = TestBatch([str(m)])
            test_q.put(batch)
        test_q.put(None)

    def mock_results_q_pull(results_q: JoinableQueue):
        states = {}
        expected_nodeids = [str(m) for m in range(100)]
        has_error = False
        for m in range(100):
            result_list = results_q.get()
            if result_list is None:
                break
            results_q.task_done()
            for result in result_list:
                if isinstance(result, ResultTestStatus):
                    has_error = has_error or result.report.outcome != 'passed'
                    has_error = has_error or result.report.nodeid not in expected_nodeids
                elif isinstance(result, TestState):
                    assert result.test_id not in states.get(result.state, {})
                    states.setdefault(result.state, {})[result.test_id] = result
        assert not has_error, "Invalid outcome or nodeid received"
        assert not states.get(TestStateEnum.RETRY), f"Expected no retries: {states[TestStateEnum.RETRY]}"
        assert states.get(TestStateEnum.STARTED)
        assert states.get(TestStateEnum.FINISHED)
        assert set(states[TestStateEnum.STARTED].keys()) == set(states[TestStateEnum.FINISHED].keys())
        assert set(states[TestStateEnum.STARTED].keys()) == {str(m) for m in range(100)}

    def mock_pytest_runtest_protocol(*args, **kwargs):
        item = kwargs['item']
        items_ran[item.nodeid] = kwargs['item']
        report = TestReport(nodeid=item.nodeid,
                            location=('', None, ''),
                            keywords={},
                            outcome='passed',
                            longrepr=None,
                            when='call')
        session.pytest_runtest_logreport(report)

    @dataclass
    class Hook:
        pytest_runtest_protocol = mock_pytest_runtest_protocol

    @dataclass
    class Config:
        hook: Hook = Hook()

    @dataclass
    class PytestNode:
        nodeid: str
        config: Config = Config()

    @dataclass
    class PytestSession:
        items: List[PytestNode]
        config: Config = Config()
        testsfailed: bool = False
        shouldstop: bool = False

    pytest_session = PytestSession([PytestNode(str(n)) for n in range(100)])
    session.pytest_collection_finish(pytest_session)
    populate_proc = multiprocessing.Process(target=mock_test_q_populate, args=(test_q, ))
    populate_proc.start()
    pull_proc = multiprocessing.Process(target=mock_results_q_pull, args=(results_q, ))
    pull_proc.start()
    session.test_loop(pytest_session)
    results_q.put(None)
    populate_proc.join(timeout=1)
    pull_proc.join(timeout=10)
    assert populate_proc.exitcode == 0
    assert pull_proc.exitcode == 0


def test_system_worker_e2e():
    m = multiprocessing.Manager()
    test_q = m.JoinableQueue(10)
    results_q = m.JoinableQueue(10)
    cwd = os.getcwd()
    os.chdir(WORKER_TEST_DIR)
    global_mgr_port = _find_free_port()
    authkey = b"1234546"
    agent_authkey = b'abcdef'
    global_mgr = Global.Manager.as_server(address=('localhost', global_mgr_port), auth_key=authkey)
    agent_port = _find_free_port()
    agent = WorkerAgent.as_server(('localhost', agent_port), authkey=agent_authkey)
    session_id = "Session1"

    def external_process(test_q: JoinableQueue, results_q: JoinableQueue):
        agent_client = WorkerAgent.as_client(address=('localhost', agent_port), authkey=agent_authkey)
        agent_client.start_session(session_id=session_id,
                                   test_q=test_q, results_q=results_q, global_mgr_address=('localhost', global_mgr_port),
                                   args=['-s', '--cores', '2'],
                                   authkey=authkey)
        agent_client.start_worker(session_id=session_id, worker_id="Worker-1")

    proc = multiprocessing.Process(
        target=external_process,
        args=(test_q, results_q)
    )
    try:
        proc.start()
        batch = TestBatch(['test_em.py::test1', "test_em.py::test2"])
        test_q.put(batch)
        test_q.put(None)
        results = results_q.get()
        results_q.task_done()
        while True:
            try:
                results = results_q.get(timeout=1)
                # TODO: add check on results that are accumulated
                results_q.task_done()
            except:
                break
        proc.join(timeout=5)
        assert proc.exitcode is not None
    finally:
        results_q.join()
        global_mgr.shutdown()
        proc.terminate()
        agent.shutdown_session(session_id=session_id, timeout=2.0)
        agent.shutdown()
        os.chdir(cwd)
