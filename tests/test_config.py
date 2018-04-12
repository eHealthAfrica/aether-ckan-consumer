import unittest
import os

from consumer import config as Config


class TestConfig(unittest.TestCase):
    def test_read_file(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config.json'

        contents = Config.read_file(dir_path, file_name)

        assert len(contents) > 0

    def test_read_file_non_existing(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'non_existing_file.json'

        with self.assertRaises(SystemExit) as context:
            Config.read_file(dir_path, file_name)

    def test_parse_json_from_file(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config.json'

        contents = Config.parse_json_from_file(
            dir_path,
            file_name
        )

        assert type(contents) == dict
        assert contents.get('foo') == 1

    def test_parse_json_from_file_malformed(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        file_name = 'config_malformed.json'

        with self.assertRaises(SystemExit) as context:
            Config.parse_json_from_file(dir_path, file_name)

    def test_validate_config(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        config_file = 'config.json'
        schema_file = 'config.schema'

        config = Config.validate_config(
            dir_path,
            config_file,
            schema_file
        )

    def test_validate_config_not_valid(self):
        dir_path = os.path.join(os.getcwd(), 'tests', 'fixtures')
        config_file = 'config_not_valid.json'
        schema_file = 'config.schema'

        with self.assertRaises(SystemExit) as context:
            Config.validate_config(
                dir_path,
                config_file,
                schema_file
            )

    def test_get_config(self):
        self.test_validate_config()

        config = Config.get_config()

        assert type(config) is dict
        assert config.get('foo') == 1
