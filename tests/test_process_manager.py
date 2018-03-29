import unittest
import os

from mock import Mock

from consumer.process_manager import ProcessManager
from consumer.server_manager import ServerManager


class TestProcessManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestProcessManager, self).__init__(*args, **kwargs)

        self.process_manager = ProcessManager()

    def test_run(self):
        self.process_manager.listen_stop_signal = Mock()
        self.process_manager.validate_config = Mock()
        self.process_manager.spawn_server_managers = Mock()
        self.process_manager.run()

        assert self.process_manager.listen_stop_signal.called
        assert self.process_manager.validate_config.called
        assert self.process_manager.spawn_server_managers.called

    def test_read_file(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config.json'

        contents = self.process_manager.read_file(dir_path, file_name)

        assert len(contents) > 0

    def test_read_file_non_existing(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'non_existing_file.json'

        with self.assertRaises(SystemExit) as context:
            self.process_manager.read_file(dir_path, file_name)

    def test_parse_json_from_file(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config.json'

        contents = self.process_manager.parse_json_from_file(
            dir_path,
            file_name
        )

        assert type(contents) == dict
        assert contents.get('foo') == 1

    def test_parse_json_from_file_malformed(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config_malformed.json'

        with self.assertRaises(SystemExit) as context:
            self.process_manager.parse_json_from_file(dir_path, file_name)

    def test_validate_config(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        config_file = 'config.json'
        schema_file = 'config.schema'

        config = self.process_manager.validate_config(
            dir_path,
            config_file,
            schema_file
        )

        assert type(config) is dict
        assert config.get('foo') == 1

    def test_validate_config_not_valid(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        config_file = 'config_not_valid.json'
        schema_file = 'config.schema'

        with self.assertRaises(SystemExit) as context:
            self.process_manager.validate_config(
                dir_path,
                config_file,
                schema_file
            )

    def test_spawn_server_managers(self):
        config = {
            'ckan_servers': [
                {'url': 'https://demo.ckan.org', 'datasets': []},
                {'url': 'https://beta.ckan.org', 'datasets': []}
            ]
        }

        self.process_manager.spawn_server_managers(config)

        assert len(self.process_manager.server_managers) == \
            len(config.get('ckan_servers'))
        assert type(self.process_manager.server_managers[0]) is ServerManager
        assert type(self.process_manager.server_managers[1]) is ServerManager
