import os
from pathlib import Path

import pytest

from pytest_mproc.ptmproc_data import ProjectConfig, RemoteHostConfig


class TestProjectConfig:
    ROOT = Path(__file__).parent / "resources"

    def test_from_file(self):
        project = ProjectConfig.from_file(self.ROOT / "project.cfg")
        assert project.requirements_paths == [(self.ROOT / "./requirements1.txt").resolve(),
                                              (self.ROOT / "./requirements2.txt").resolve()]
        assert project.resource_paths == [(self.ROOT / "resources1").resolve(),
                                          (self.ROOT / "resources2").resolve()]
        assert project.src_paths == [(self.ROOT / "src1").resolve(),
                                     (self.ROOT / "src2").resolve()]
        assert project.tests_path == (self.ROOT / "project_tests").resolve()

    def test_from_file_invalid_json(self):
        with pytest.raises(pytest.UsageError) as e:
            ProjectConfig.from_file(self.ROOT / "project_invalid_json.cfg")
        assert "Invalid json" in str(e)

    @pytest.mark.parametrize("key", ['requirements_paths', 'resource_paths', 'src_paths'])
    def test_from_file_no_such_dir_or_file(self, key):
        with pytest.raises(pytest.UsageError) as e:
            ProjectConfig.from_file((self.ROOT / f"project_{key}_no_such_dir.cfg"))
        assert "is not a path" in str(e)

    def test_from_file_no_such_tests_path(self):
        with pytest.raises(pytest.UsageError) as e:
            ProjectConfig.from_file((self.ROOT / "project_tests_path_no_such_file.cfg"))
        assert "does not exist" in str(e)


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
