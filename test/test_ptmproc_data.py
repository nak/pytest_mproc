import os
from pathlib import Path

import pytest

from pytest_mproc.ptmproc_data import ProjectConfig, RemoteHostConfig
from pytest_mproc.remote.bundle import Bundle


class TestProjectConfig:
    ROOT = Path(__file__).parent / "resources"

    def test_from_file(self):
        project = ProjectConfig.from_file(self.ROOT / "project.cfg")
        assert project.src_paths == [Path("./src1"), Path("src2")]
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

    @pytest.mark.parametrize("key", ['src_paths', 'test_files'])
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
        config = RemoteHostConfig.from_list(
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
            RemoteHostConfig.from_list(
                ['129.1.2.10;device=DEVICE1;arg',
                 '129.1.2.11;device=DEVICE2']
            )

    @pytest.mark.asyncio
    async def test_from_uri_string(self):
        os.chdir(Path(__file__).parent)
        string = "file://./resources/remote_host.cfg"
        configs = {}
        it = RemoteHostConfig.from_uri_string(string)
        async for config in it:
            configs[config.remote_host] = config
        assert '192.1.3.64' in configs
        assert '192.1.3.65' in configs
        assert configs['192.1.3.64'].remote_host == '192.1.3.64'
        assert configs['192.1.3.64'].arguments == {"device": "DEVICE1"}
        assert configs['192.1.3.65'].remote_host == '192.1.3.65'
        assert configs['192.1.3.65'].arguments == {"device": "DEVICE2", 'arg': "VAL"}
