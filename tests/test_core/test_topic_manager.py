import unittest

from consumer import db
from consumer.core.topic_manager import TopicManager


class TestTopicManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestTopicManager, self).__init__(*args, **kwargs)

        url = 'sqlite://'
        db.init(url)

        config = {
            'topic': {
                'name': 'test',
                'number_of_consumers': 1
            },
            'server_name': 'Test server',
            'dataset_name': 'test',
            'resource_name': 'test',
        }

        self.topic_manager = TopicManager(None, config)

    def test_extract_fields_from_schema(self):
        schema = [
          {
            "fields": [
              {
                "jsonldPredicate": {
                  "_type": "@id",
                  "_id": "http://demo.eha.org/GeoLocation"
                },
                "type": [
                  "string",
                  {
                    "items": "string",
                    "type": "array"
                  }
                ],
                "name": "GeoLocation-id"
              }
            ],
            "type": "record",
            "name": "http://demo.eha.org/centralPoint"
          },
          {
            "fields": [
              {
                "jsonldPredicate": {
                  "_type": "@id",
                  "_id": "http://demo.eha.org/GeoLocation"
                },
                "type": [
                  "string",
                  {
                    "items": "string",
                    "type": "array"
                  }
                ],
                "name": "GeoLocation-id"
              }
            ],
            "type": "record",
            "name": "http://demo.eha.org/location"
          },
          {
            "fields": [
              {
                "jsonldPredicate": {
                  "_type": "@id",
                  "_id": "http://demo.eha.org/GeoLocation"
                },
                "type": [
                  "string",
                  {
                    "items": "string",
                    "type": "array"
                  }
                ],
                "name": "GeoLocation-id"
              }
            ],
            "type": "record",
            "name": "http://demo.eha.org/perimeter"
          },
          {
            "extends": "http://demo.eha.org/BaseModel",
            "type": "record",
            "name": "http://demo.eha.org/Place",
            "aetherBaseSchema": True,
            "fields": [
              {
                "jsonldPredicate": "@id",
                "type": "string",
                "name": "id",
                "inherited_from": "http://demo.eha.org/BaseModel"
              },
              {
                "jsonldPredicate": "http://demo.eha.org/centralPoint",
                "type": [
                  "null",
                  "http://demo.eha.org/centralPoint"
                ],
                "name": "centralPoint"
              },
              {
                "doc": "A description of the thing.",
                "jsonldPredicate": "http://demo.eha.org/description",
                "type": [
                  "null",
                  "string",
                  {
                    "items": "string",
                    "type": "array"
                  }
                ],
                "name": "description"
              },
              {
                "doc": "A Name",
                "jsonldPredicate": "http://demo.eha.org/name",
                "type": [
                  "null",
                  "string",
                  {
                    "items": "string",
                    "type": "array"
                  }
                ],
                "name": "name"
              },
              {
                "jsonldPredicate": "http://demo.eha.org/perimeter",
                "type": [
                  "null",
                  "http://demo.eha.org/perimeter"
                ],
                "name": "perimeter"
              },
              {
                "jsonldPredicate": "http://demo.eha.org/location",
                "type": [
                  "null",
                  "http://demo.eha.org/location"
                ],
                "name": "location"
              }
            ]
          }
        ]

        fields = self.topic_manager.extract_fields_from_schema(schema)

        assert len(fields) == 6
        assert fields[0] == {'type': 'string', 'name': 'id'}
        assert fields[1] == {
            'type': ['null', 'http://demo.eha.org/centralPoint'],
            'name': 'centralPoint'
        }
        assert fields[2] == {
            'type': ['null', 'string', {'items': 'string', 'type': 'array'}],
            'name': 'description'
        }
        assert fields[3] == {
            'type': ['null', 'string', {'items': 'string', 'type': 'array'}],
            'name': 'name'
        }
        assert fields[4] == {
            'type': ['null', 'http://demo.eha.org/perimeter'],
            'name': 'perimeter'
        }
        assert fields[5] == {
            'type': ['null', 'http://demo.eha.org/location'],
            'name': 'location'
        }

    def test_prepare_fields_for_resource(self):
        fields = [
            {'type': 'string', 'name': 'string_field'},
            {'type': 'int', 'name': 'int_field'},
            {'type': 'boolean', 'name': 'boolean_field'},
            {'type': 'long', 'name': 'long_field'},
            {'type': 'float', 'name': 'float_field'},
            {'type': 'double', 'name': 'double_field'},
            {'type': 'bytes', 'name': 'bytes_field'},
            {'type': {'type': 'record'}, 'name': 'record_field'},
            {'type': {'type': 'map'}, 'name': 'map_field'},
            {'type': ['null', 'string', 'int'], 'name': 'union_field'},
            {
                'type': {'type': 'array', 'items': 'string'},
                'name': 'array_field'
            }
        ]

        resource_fields = self.topic_manager.prepare_fields_for_resource(
            fields
        )

        assert resource_fields[0] == {'type': 'text', 'id': 'string_field'}
        assert resource_fields[1] == {'type': 'int4', 'id': 'int_field'}
        assert resource_fields[2] == {'type': 'bool', 'id': 'boolean_field'}
        assert resource_fields[3] == {'type': 'int8', 'id': 'long_field'}
        assert resource_fields[4] == {'type': 'float4', 'id': 'float_field'}
        assert resource_fields[5] == {'type': 'float8', 'id': 'double_field'}
        assert resource_fields[6] == {'type': 'bytea', 'id': 'bytes_field'}
        assert resource_fields[7] == {'type': 'json', 'id': 'record_field'}
        assert resource_fields[8] == {'type': 'json', 'id': 'map_field'}
        assert resource_fields[9] == {'type': 'text', 'id': 'union_field'}
        assert resource_fields[10] == {'type': '_text', 'id': 'array_field'}

    def test_is_field_primitive_type(self):
        assert self.topic_manager.is_field_primitive_type({
            'type': 'string'
        }) is True
        assert self.topic_manager.is_field_primitive_type({
            'type': 'boolean'
        }) is True
        assert self.topic_manager.is_field_primitive_type({
            'type': 'int'
        }) is True
        assert self.topic_manager.is_field_primitive_type({
            'type': 'long'
        }) is True
        assert self.topic_manager.is_field_primitive_type({
            'type': 'float'
        }) is True
        assert self.topic_manager.is_field_primitive_type({
            'type': 'double'
        }) is True
        assert self.topic_manager.is_field_primitive_type({
            'type': 'bytes'
        }) is True
        assert self.topic_manager.is_field_primitive_type({
            'type': 'record'
        }) is False
        assert self.topic_manager.is_field_primitive_type({
            'type': 'map'
        }) is False
        assert self.topic_manager.is_field_primitive_type({
            'type': 'array'
        }) is False
        assert self.topic_manager.is_field_primitive_type({
            'type': ['null', 'int']
        }) is False
