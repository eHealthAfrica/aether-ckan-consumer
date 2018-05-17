import unittest
import os

import pook
import sqlalchemy

from consumer.core.dataset_manager import DatasetManager
from consumer import db
from consumer.config import parse_json_from_file, validate_config


class TestDatasetManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestDatasetManager, self).__init__(*args, **kwargs)

        url = 'sqlite:////srv/app/db/test.db'
        db.init(url)

        config = {
            'ckan_url': 'http://ckan-server1.com',
            'server_name': 'Test server',
            'dataset': {
                'metadata': {
                    'title': 'Some title',
                    'name': 'Some name',
                    'notes': 'Some description',
                },
                'resources': [
                    {
                        'metadata': {
                            'title': 'Sensor data',
                            'description': 'Sensor data from wind turbines',
                            'name': 'sensor-data'
                        },
                        'topics': [
                            {
                                'name': 'wind-turbines',
                                'number_of_consumers': 1
                            }
                        ]
                    }
                ]
            }
        }

        self.dataset_manager = DatasetManager(None, config)

    def test_update_metadata_from_defaults(self):
        dir_path = os.getcwd()
        file_name = os.path.join('config', 'dataset_metadata.json')
        default_metadata = parse_json_from_file(dir_path, file_name)
        updated_metadata = self.dataset_manager.update_metadata_from_defaults(
            {
                'author': 'Overwritten author',
                'maintainer': 'Overwritten author'
            }
        )

        assert default_metadata.get('author') != updated_metadata.get('author')
        assert default_metadata.get('maintainer') != \
            updated_metadata.get('maintainer')
