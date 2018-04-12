import logging
from threading import Thread
from time import sleep
import io
import ast
import sys

from kafka import KafkaConsumer
from kafka import errors as KafkaErrors
from avro.datafile import DataFileReader
from avro.io import DatumReader

from config import get_config

CONN_RETRY = 3
CONN_RETRY_WAIT_TIME = 2

# Temporary set the log level for Kafka to ERROR during development, so that
# stdout is not bloated with messages
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.ERROR)


class TopicManager(Thread):

    def __init__(self, topic_config):
        super(TopicManager, self).__init__()

        self.logger = logging.getLogger(__name__)
        self.topic_config = topic_config

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
        group_id = 'CKAN_{0}_{1}_{2}'.format(
            server_name,
            dataset_name,
            topic_name
        )

        for i in range(CONN_RETRY):
            try:
                self.consumer = KafkaConsumer(
                    group_id=group_id,
                    bootstrap_servers=[kafka_url],
                    auto_offset_reset='latest',
                )

                return True
            except KafkaErrors.NoBrokersAvailable:
                self.logger.error('Kafka not available. Retrying...')
                sleep(CONN_RETRY_WAIT_TIME)

        self.logger.error('Could not connect to Kafka.')

        return False

    def create_kafka_consumer_group(self):
        pass

    def poll_messages(self):
        messages = self.consumer.poll(timeout_ms=1000)

        return messages

    def read_messages(self):
        while True:
            partitioned_messages = self.poll_messages()

            if partitioned_messages:
                for part, packages in partitioned_messages.items():
                    for package in packages:
                        obj = io.BytesIO()
                        obj.write(package.value)
                        reader = DataFileReader(obj, DatumReader())
                        schema = self.extract_schema(reader)

                        print 'Message from topic: {0}'.format(package.topic)
                        print 'Thread name: ', self.getName()
                        print 'Schema:', schema

                        for x, msg in enumerate(reader):
                            print 'Data:', msg

                        obj.close()

            sleep(1)

    def extract_schema(self, reader):
        raw_schema = ast.literal_eval(str(reader.meta))
        schema = ast.literal_eval(str(raw_schema.get('avro.schema')))

        return schema
