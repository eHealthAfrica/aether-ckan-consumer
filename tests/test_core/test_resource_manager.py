import unittest

from consumer import db
from consumer.core.resource_manager import ResourceManager


class TestResourceManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestResourceManager, self).__init__(*args, **kwargs)

        url = 'sqlite:////srv/app/db/test.db'
        db.init(url)

        config = {
            'ckan_url': 'http://ckan-server1.com',
            'server_name': 'Test server',
            'resource': {
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
            },
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

        self.resource_manager = ResourceManager(None, config)

    def test_get_schema_changes_add_field(self):
        schema = [
            {'id': 'name', 'type': 'text'},
            {'id': 'age', 'type': 'int4'},
        ]

        fields = [
            {'id': 'name', 'type': 'text'},
            {'id': 'age', 'type': 'int4'},
            {'id': 'gender', 'type': 'text'},
        ]

        schema_changes = self.resource_manager.get_schema_changes(
            schema, fields
        )

        assert len(schema_changes) == 1
        assert schema_changes[0] == {'id': 'gender', 'type': 'text'}

    def test_get_schema_changes_remove_field(self):
        schema = [
            {'id': 'name', 'type': 'text'},
            {'id': 'age', 'type': 'int4'},
        ]

        fields = [
            {'id': 'name', 'type': 'text'},
        ]

        schema_changes = self.resource_manager.get_schema_changes(
            schema, fields
        )

        assert len(schema_changes) == 0

    def test_get_schema_changes_same_fields(self):
        schema = [
            {'id': 'name', 'type': 'text'},
            {'id': 'age', 'type': 'int4'},
        ]

        fields = [
            {'id': 'name', 'type': 'text'},
            {'id': 'age', 'type': 'int4'},
        ]

        schema_changes = self.resource_manager.get_schema_changes(
            schema, fields
        )

        assert len(schema_changes) == 0

    def test_convert_item_to_array(self):
        self.resource_manager.schema = [
            {'id': 'name', 'type': 'text'},
            {'id': 'scores', 'type': '_int4'},
        ]

        records = [
            {'name': 'Aleksandar', 'scores': [10, 11]},
            {'name': 'Ana', 'scores': 8},
        ]

        records = self.resource_manager.convert_item_to_array(records)

        assert len(records) == 2
        assert records[1].get('scores') == [8]
