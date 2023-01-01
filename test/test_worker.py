import multiprocessing
import os
from queue import Empty
from dataclasses import dataclass, field
from multiprocessing import JoinableQueue
from pathlib import Path
from typing import List

from _pytest.reports import TestReport

from pytest_mproc import find_free_port
from pytest_mproc.data import TestBatch, StatusTestState, TestStateEnum, WorkerExited
from pytest_mproc.fixtures import Global
from pytest_mproc.user_output import always_print
from pytest_mproc.worker import WorkerSession, WorkerAgent

WORKER_TEST_DIR = Path(__file__).parent / "resources" / "project_tests"


def test__put():
    test_q = JoinableQueue()
    status_q = JoinableQueue()
    report_q = JoinableQueue()
    session = WorkerSession(session_id="Session1",
                            worker_id="Worker-1-1", test_q=test_q, status_q=status_q, report_q=report_q)
    n = 1
    for _ in range(4):
        session._put(n)
        assert status_q.empty()
        n += 1
    session._put(n)
    assert status_q.get() == [m + 1 for m in range(5)]


def test_test_loop():
    test_q = JoinableQueue(10)
    status_q = JoinableQueue(10)
    report_q = JoinableQueue(10)
    session = WorkerSession(session_id="Session1",
                            worker_id="Worker-1-1", test_q=test_q, status_q=status_q, report_q=report_q)
    items_ran = {}

    def mock_test_q_populate(test_q: JoinableQueue):
        for m in range(100):
            batch = TestBatch([str(m)])
            test_q.put(batch)
        test_q.put(None)

    def mock_status_q_pull(status_q: JoinableQueue):
        states = {}
        expected_nodeids = [str(m) for m in range(100)]
        has_error = False
        for m in range(100):
            status_list = status_q.get()
            if status_list is None:
                break
            status_q.task_done()
            for status in status_list:
                if isinstance(status, TestReport):
                    has_error = has_error or status.report.outcome != 'passed'
                    has_error = has_error or status.report.nodeid not in expected_nodeids
                elif isinstance(status, StatusTestState):
                    assert status.test_id not in states.get(status.state, {})
                    states.setdefault(status.state, {})[status.test_id] = status
        assert not has_error, "Invalid outcome or nodeid received"
        assert not states.get(TestStateEnum.RETRY), f"Expected no retries: {states[TestStateEnum.RETRY]}"
        assert states.get(TestStateEnum.STARTED)
        assert states.get(TestStateEnum.FINISHED)
        assert set(states[TestStateEnum.STARTED].keys()) == set(states[TestStateEnum.FINISHED].keys())
        assert set(states[TestStateEnum.STARTED].keys()) == {str(m) for m in range(100)}

    def mock_report_q_pull(report_q: JoinableQueue):
        report = report_q.get()
        while report is not None:
            report = report_q.get()

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
        hook: Hook = field(default_factory=Hook)

    @dataclass
    class PytestNode:
        nodeid: str
        config: Config = field(default_factory=Config)

    @dataclass
    class PytestSession:
        items: List[PytestNode]
        config: Config = field(default_factory=Config)
        testsfailed: bool = False
        shouldstop: bool = False

    pytest_session = PytestSession([PytestNode(str(n)) for n in range(100)])
    session.pytest_collection_finish(pytest_session)
    populate_proc = multiprocessing.Process(target=mock_test_q_populate, args=(test_q, ))
    populate_proc.start()
    pull_proc = multiprocessing.Process(target=mock_status_q_pull, args=(status_q, ))
    pull_proc.start()
    report_pull_proc = multiprocessing.Process(target=mock_report_q_pull, args=(report_q,))
    report_pull_proc.start()
    session.test_loop(pytest_session)
    status_q.put(None)
    report_q.put(None)
    populate_proc.join(timeout=1)
    pull_proc.join(timeout=10)
    report_pull_proc.join(timeout=1)
    assert populate_proc.exitcode == 0
    assert pull_proc.exitcode == 0


# noinspection PyUnresolvedReferences
def test_system_worker_e2e():
    m = multiprocessing.Manager()
    test_q = m.JoinableQueue(20)
    status_q = m.JoinableQueue(20)
    report_q = m.JoinableQueue(20)
    cwd = os.getcwd()
    os.chdir(WORKER_TEST_DIR)
    global_mgr_port = find_free_port()
    authkey = b"1234546"
    agent_authkey = b'abcdef'
    global_mgr = Global.Manager.as_server(address=('localhost', global_mgr_port), auth_key=authkey)
    agent_port = find_free_port()
    agent = WorkerAgent.start_server(('localhost', agent_port), authkey=agent_authkey)
    session_id = "Session1"

    def external_process(test_q: JoinableQueue, status_q: JoinableQueue, report_q: JoinableQueue):
        import sys
        for path in sys.path.copy():
            if Path(path).absolute() == Path(__file__).parent.absolute():
                sys.path.remove(path)
            if Path(path).absolute() == Path(__file__).parent.parent.absolute():
                sys.path.remove(path)
        os.environ['PYTHONPATH'] = str(Path(__file__).parent.parent / 'src')
        always_print(f">>>>>>>>>>>>>>>>>> S{str(Path(__file__).parent)}")
        agent_client = WorkerAgent.as_client(address=('localhost', agent_port), authkey=agent_authkey)
        agent_client.start_session(session_id=session_id,
                                   main_address=('localhost', global_mgr_port),
                                   main_token=authkey.hex(),
                                   token=authkey.hex(),
                                   test_q=test_q, status_q=status_q, report_q=report_q,
                                   global_mgr_address=('localhost', global_mgr_port),
                                   args=['-s', '--cores', '2', f'--ignore={str(Path(__file__).parent)}'],
                                   cwd=Path(os.getcwd()),)
        agent_client.start_worker(session_id=session_id, worker_id="Worker-1")

    proc = multiprocessing.Process(
        target=external_process,
        args=(test_q, status_q, report_q)
    )
    try:
        proc.start()
        batch = TestBatch(['test_em.py::test1', "test_em.py::test2"])
        test_q.put(batch)
        test_q.put(None)
        while True:
            status = status_q.get(timeout=5)
            status_q.task_done()
            if isinstance(status, list):
                for stat in status:
                    if isinstance(stat, WorkerExited):
                        status = stat
                        break
            if isinstance(status, WorkerExited):
                break
            # TODO: add check on results that are accumulated
        nodeids = set()
        # we sized the buffers to allow serializing getting of status and report queues, normally these are processed
        # in parallel
        while True:
            report = report_q.get(timeout=1)
            report_q.task_done()
            if isinstance(report, WorkerExited):
                break
            assert report.nodeid.rsplit('/', maxsplit=1)[-1] in ['test_em.py::test1', "test_em.py::test2"]
            nodeids.add(report.nodeid.rsplit('/', maxsplit=1)[-1])
        assert nodeids == {'test_em.py::test1', "test_em.py::test2"}
        proc.join(timeout=5)
        assert proc.exitcode is not None
    finally:
        global_mgr.shutdown()
        proc.terminate()
        agent.shutdown_session(address=('localhost', agent_port), session_id=session_id, timeout=2.0)
        agent.shutdown()
        os.chdir(cwd)
