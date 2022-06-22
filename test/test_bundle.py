from pathlib import Path

from pytest_mproc.ptmproc_data import RemoteWorkerConfig, _determine_cli_args


def test__determine_cli_args():
    worker_config = RemoteWorkerConfig(remote_root=Path("/just/a/test"), arguments={'device': 'XXXXXX',
                                                                                  'cores': '2',
                                                                                  'ENV_VAR': 'SOMETHING'},
                                       remote_host='some.host'
                                       )
    args, env = _determine_cli_args(worker_config, ptmproc_args={'--cores': int}, sys_args=['--cores', '1'])
    assert args == ['--device', '"XXXXXX"', '--cores', '"2"'], f"{args} not as expectged"
    assert env == {'ENV_VAR': '"SOMETHING"'}

