import unittest
import os

from mock import Mock

from consumer.process_manager import ProcessManager


class TestProcessManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestProcessManager, self).__init__(*args, **kwargs)

        self.processManager = ProcessManager()

    def test_run(self):
        self.processManager.listen_stop_signal = Mock()
        self.processManager.validate_config = Mock()
        self.processManager.run()

        assert self.processManager.listen_stop_signal.called
        assert self.processManager.validate_config.called

    def test_read_file(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config.json'

        contents = self.processManager.read_file(dir_path, file_name)

        assert len(contents) > 0

    def test_read_file_non_existing(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'non_existing_file.json'

        with self.assertRaises(SystemExit) as context:
            self.processManager.read_file(dir_path, file_name)

    def test_parse_json_from_file(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config.json'

        contents = self.processManager.parse_json_from_file(
            dir_path,
            file_name
        )

        assert type(contents) == dict
        assert contents.get('foo') == 1

    def test_parse_json_from_file_malformed(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config_malformed.json'

        with self.assertRaises(SystemExit) as context:
            self.processManager.parse_json_from_file(dir_path, file_name)

    def test_validate_config(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        config_file = 'config.json'
        schema_file = 'config.schema'

        config = self.processManager.validate_config(
            dir_path,
            config_file,
            schema_file
        )

        assert type(config) == dict
        assert config.get('foo') == 1

    def test_validate_config_not_valid(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        config_file = 'config_not_valid.json'
        schema_file = 'config.schema'

        with self.assertRaises(SystemExit) as context:
            self.processManager.validate_config(
                dir_path,
                config_file,
                schema_file
            )
