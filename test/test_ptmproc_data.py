import asyncio
import json
import multiprocessing
import os

import pytest

from aiohttp import web
from pathlib import Path

from aiohttp.web_exceptions import HTTPNotFound

from pytest_mproc.http import HTTPSession
from pytest_mproc.ptmproc_data import ProjectConfig, RemoteWorkerConfig
from pytest_mproc.remote.bundle import Bundle
from pytest_mproc.user_output import always_print


class Server:
    
    def __init__(self, app: web.Application, is_primary: bool):
        self._app = app
        self._timer_task = None
        if is_primary:
            app.add_routes([web.get('/start_session', self.star_session_fail)])
        else:
            app.add_routes([web.get('/start_session', self.start_session)])
        app.add_routes([web.get('/create_session', self.create_session),
                        web.get('/end_session', self.end_session),
                        web.get('/hearbeat', self.heartbeat)])

    async def star_session_fail(self, request):
        raise HTTPNotFound(reason="Explicitly fail for test purposes")

    async def create_session(self, request: web.Request):
        return web.Response(text=json.dumps({'session_id': 'session123',
                'end_session_url': f'http://{request.host}/end_session?session_id=session123',
                'start_session_url': f'http://{request.host}/start_session?session_id=session123',
                'heartbeat_url': f'http://{request.host}/heartbeat?session_id=session123'}))
        
    async def start_session(self,  request: web.Request):
        assert request.query.get('session_id') == 'session123'
        configs = [
            {'host': '192.168.2.2', 'arguments': {'device': 'DEVICE1'}},
            {'host': '192.168.2.2', 'arguments': {'device': 'DEVICE2'}},
            {'host': '192.168.2.3', 'arguments': {'device': 'DEVICE3'}},
        ]
        response = web.StreamResponse()
        response.content_type = 'text/plain'
        await response.prepare(request)
        for config in configs:
            json_bytes = json.dumps(config).encode('utf-8') + b'\n'
            await response.write(json_bytes)
        self._timer_task = asyncio.create_task(self._timer())
        return response

    async def _timer(self):
        await asyncio.sleep(2)
        await self.end_session(None)
    
    async def heartbeat(self, request):
        if self._timer_task is not None:
            self._timer_task.cancel()
        
        self._timer_task = asyncio.create_task(self._timer())
        return web.Response(text="OK")
        
    async def end_session(self, request):
        if request is not None:
            session_id = request.match_info.get('session_id')
            assert session_id == 'session123'
        return web.Response(text="{'status': 'OK'}")

    def startup(self, port: int):
        web.run_app(self._app, host='localhost', port=port)


@pytest.fixture()
def primary_server():
    app = web.Application()
    server = Server(app, is_primary=True)
    port = 9002
    proc = multiprocessing.Process(target=server.startup, args=(port, ))
    proc.start()
    url = f"http://localhost:{port}/create_session"
    try:
        yield url
    finally:
        proc.kill()



@pytest.fixture()
def secondary_server():
    app = web.Application()
    server = Server(app, is_primary=False)
    port = 9003
    proc = multiprocessing.Process(target=server.startup, args=(port, ))
    proc.start()
    try:
        yield f"http://localhost:{port}/create_session"
    finally:
        proc.kill()


class TestProjectConfig:
    ROOT = Path(__file__).parent / "resources"

    def test_from_file(self):
        project = ProjectConfig.from_file(self.ROOT / "project.cfg")
        assert project.test_files == [
            Path("project_tests/*.py"),
            Path("./requirements1.txt"),
            Path("./requirements2.txt"),
            Path("./resources1/*"),
            Path("resources2/*"),
        ]

    def test_from_file_invalid_json(self):
        with pytest.raises(pytest.UsageError) as e:
            ProjectConfig.from_file(self.ROOT / "project_invalid_json.cfg")
        assert "Invalid json" in str(e)

    @pytest.mark.parametrize("key", ['test_files'])
    def test_from_file_no_such_dir_or_file(self, chdir, key):
        chdir.chdir(self.ROOT)
        with pytest.raises(Exception) as e:
            project_config = ProjectConfig.from_file((self.ROOT / f"project_{key}_no_such_dir.cfg"))
            Bundle.create(self.ROOT, project_config)
        if isinstance(e.value, pytest.UsageError):
            assert "is not a path" in str(e)
        elif isinstance(e.value, ValueError):
            assert "did not match" in str(e)
        else:
            raise e.value


class TestRemoteHostConfig:
    def test_from_list(self):
        config = RemoteWorkerConfig.from_list(
            ['129.1.2.10;device=DEVICE1;arg2=VALUE',
             '129.1.2.11;device=DEVICE2']
        )
        configs = {config.remote_host: config for config in config}
        assert '129.1.2.10' in configs
        assert '129.1.2.11' in configs
        assert configs['129.1.2.10'].remote_host == '129.1.2.10'
        assert configs['129.1.2.10'].arguments == {'device': "DEVICE1",
                                                   'arg2': "VALUE"}
        assert configs['129.1.2.11'].remote_host == '129.1.2.11'
        assert configs['129.1.2.11'].arguments == {'device': "DEVICE2"}

    def test_from_list_bad_string(self):
        with pytest.raises(pytest.UsageError):
            RemoteWorkerConfig.from_list(
                ['129.1.2.10;device=DEVICE1;arg',
                 '129.1.2.11;device=DEVICE2']
            )

    @pytest.mark.asyncio
    async def test_from_file_uri_string(self):
        os.chdir(Path(__file__).parent)
        string = "file://./resources/remote_host.cfg"
        configs = {}
        it = RemoteWorkerConfig.from_uri_string(string)
        async for config in it:
            configs[config.remote_host] = config
        assert '192.1.3.64' in configs
        assert '192.1.3.65' in configs
        assert configs['192.1.3.64'].remote_host == '192.1.3.64'
        assert configs['192.1.3.64'].arguments == {"device": "DEVICE1"}
        assert configs['192.1.3.65'].remote_host == '192.1.3.65'
        assert configs['192.1.3.65'].arguments == {"device": "DEVICE2", 'arg': "VAL"}

    @pytest.mark.asyncio
    async def test_from_http_uri_string(self, primary_server, secondary_server):
        config_urls = [primary_server, secondary_server]
        configs = RemoteWorkerConfig.from_raw_list(config_urls)
        index = 1
        async for config in configs:
            assert config.remote_host == '192.168.2.2' if index <= 2 else '192.168.2.3'
            assert config.arguments == {'device': f'DEVICE{index}'}
            index += 1
