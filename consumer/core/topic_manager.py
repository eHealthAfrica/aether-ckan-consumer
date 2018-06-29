import ast
import logging
from threading import Thread
from time import sleep
import io
import json
import sys

from kafka import KafkaConsumer
from kafka import errors as KafkaErrors
from avro.datafile import DataFileReader
from avro.io import DatumReader

from consumer.config import get_config

CONN_RETRY = 3
CONN_RETRY_WAIT_TIME = 2

avroToPostgresPrimitiveTypes = {
    'string': 'text',
    'boolean': 'bool',
    'int': 'int4',
    'long': 'int8',
    'float': 'float4',
    'double': 'float8',
    'bytes': 'bytea',
}

# Temporary set the log level for Kafka to ERROR during development, so that
# stdout is not bloated with messages
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.ERROR)


class TopicManager(Thread):

    def __init__(self, resource_manager, topic_config):
        super(TopicManager, self).__init__()

        self.logger = logging.getLogger(__name__)
        self.topic_config = topic_config
        self.definition_names = []
        self.resource_manager = resource_manager
        self.stopped = False

    def run(self):
        self.create_kafka_consumer()

        if self.consumer:
            topic_name = self.topic_config.get('topic').get('name')
            self.consumer.subscribe([topic_name])
            self.logger.info(
                'Subscribed to topic "{0}" from Topic Manager "{1}"'
                .format(topic_name, self.getName())
            )
            self.read_messages()

    def create_kafka_consumer(self):
        kafka_url = get_config().get('kafka').get('url')
        server_name = self.topic_config.get('server_name')
        dataset_name = self.topic_config.get('dataset_name')
        topic_name = self.topic_config.get('topic').get('name')
        resource_name = self.topic_config.get('resource_name')
        group_id = 'CKAN_{0}_{1}_{2}_{3}'.format(
            '-'.join(server_name.split(' ')),
            dataset_name,
            resource_name,
            topic_name
        )

        for i in range(CONN_RETRY):
            try:
                self.consumer = KafkaConsumer(
                    group_id=group_id,
                    bootstrap_servers=[kafka_url],
                    auto_offset_reset='earliest',
                )

                return True
            except KafkaErrors.NoBrokersAvailable:
                self.logger.error('Kafka not available. Retrying...')
                sleep(CONN_RETRY_WAIT_TIME)

        self.logger.error('Could not connect to Kafka.')

        return False

    def poll_messages(self):
        messages = self.consumer.poll(timeout_ms=1000)

        return messages

    def read_messages(self):
        while True:
            if self.stopped:
                self.consumer.close()
                self.resource_manager.on_topic_exit(self.name)
                break

            partitioned_messages = self.poll_messages()

            if partitioned_messages:
                for part, packages in partitioned_messages.items():
                    for package in packages:
                        obj = io.BytesIO()
                        obj.write(package.value)
                        reader = DataFileReader(obj, DatumReader())
                        schema = self.extract_schema(reader)

                        fields = \
                            self.extract_fields_from_schema(schema)
                        fields = self.prepare_fields_for_resource(fields)

                        records = []

                        for x, msg in enumerate(reader):
                            records.append(msg)

                        self.resource_manager.send_data_to_datastore(
                            fields,
                            records
                        )
                        obj.close()

            sleep(1)

    def extract_schema(self, reader):
        raw_schema = ast.literal_eval(str(reader.meta))
        schema = json.loads(raw_schema.get("avro.schema"))
        return schema

    def extract_fields_from_schema(self, schema):
        fields = []
        if isinstance(schema, list):
            for definition in schema:
                is_base_schema = definition.get('aetherBaseSchema')

                if is_base_schema:
                    for field in definition.get('fields'):
                        fields.append({
                            'name': field.get('name'),
                            'type': field.get('type'),
                        })

                else:
                    self.definition_names.append(definition.get('name'))
        else:
            for field in schema.get('fields'):
                fields.append({
                    'name': field.get('name'),
                    'type': field.get('type'),
                })

        return fields

    def prepare_fields_for_resource(self, fields):
        resource_fields = []

        for field in fields:
            resource_field_type = None

            if self.is_field_primitive_type(field):
                resource_field_type = \
                    avroToPostgresPrimitiveTypes.get(field.get('type'))
            elif type(field.get('type')) is dict:
                field_type = field.get('type').get('type')

                if field_type == 'record' or field_type == 'map':
                    resource_field_type = 'json'
                elif field_type == 'array':
                    field_type = field.get('type').get('items')
                    resource_field_type = '_{0}'.format(
                        avroToPostgresPrimitiveTypes.get(field_type)
                    )
                elif field_type == 'enum':
                    resource_field_type = \
                        avroToPostgresPrimitiveTypes.get('string')
            elif type(field.get('type')) is list:
                union_types = field.get('type')

                for union_type in union_types:
                    if union_type in self.definition_names or \
                       (type(union_type) is dict and
                            union_type.get('type') == 'map'):
                        resource_field_type = 'json'
                        break

                if resource_field_type:
                    resource_fields.append({
                        'type': resource_field_type,
                        'id': field.get('name'),
                    })
                    continue

                for union_type in union_types:
                    if (type(union_type) is dict and
                            union_type.get('type') == 'array'):
                        field_type = union_type.get('items')
                        resource_field_type = '_{0}'.format(
                            avroToPostgresPrimitiveTypes.get(field_type)
                        )
                        break

                if resource_field_type:
                    resource_fields.append({
                        'type': resource_field_type,
                        'id': field.get('name'),
                    })
                    continue

                if 'bytes' in union_types:
                    resource_field_type = \
                        avroToPostgresPrimitiveTypes.get('bytes')
                elif 'string' in union_types:
                    resource_field_type = \
                        avroToPostgresPrimitiveTypes.get('string')
                else:
                    resource_field_type = \
                        avroToPostgresPrimitiveTypes.get(union_types[1])

            if resource_field_type:
                resource_fields.append({
                    'type': resource_field_type,
                    'id': field.get('name'),
                })

        return resource_fields

    def is_field_primitive_type(self, field):
        field_type = field.get('type')

        if field_type in avroToPostgresPrimitiveTypes.keys():
            return True

        return False

    def stop(self):
        self.stopped = True
