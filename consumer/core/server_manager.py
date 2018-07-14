from time import sleep
import logging
import re
import requests
from requests.exceptions import ConnectionError
from threading import Thread

from aet.consumer import KafkaConsumer
from kafka.consumer.fetcher import NoOffsetForPartitionError

from consumer.core.dataset_manager import DatasetManager
from consumer.config import get_config

CONN_RETRY = 3
CONN_RETRY_WAIT_TIME = 2


class ServerManager(object):

    def __init__(self, process_manager, server_config):
        self.logger = logging.getLogger(__name__)
        self.server_config = server_config
        self.process_manager = process_manager
        self.dataset_managers = []
        self.ignored_topics = []
        self.autoconfig_watcher = None

    def check_server_availability(self, server_config):
        ''' Checks the server availability using CKAN's API action
        "status_show". Retry logic exist with a wait time between retries.

        :param server_config: The configuration for the server.
        :type server_config: dictionary

        :returns: Is server available.
        :rtype: boolean

        '''

        server_url = server_config.get('url')
        url = '{0}/api/action/status_show'.format(server_url)
        response = None

        for i in range(CONN_RETRY):
            try:
                response = requests.get(url)
                break
            except ConnectionError:
                self.logger.error('Server {0} not available. Retrying...'
                                  .format(url))
                sleep(CONN_RETRY_WAIT_TIME)

        if response is None:
            self.logger.error('Server {0} not available.'
                              .format(url))
            return False

        if response.status_code != 200:
            self.logger.error('Response for {0} not successful.'.format(url))
            return False

        try:
            data = response.json()
        except (ValueError, TypeError):
            self.logger.error('Expected JSON response for {0}.'.format(url))
            return False

        if data.get('success') is True:
            self.logger.info('Server {0} available.'.format(url))
            return True

        return False

    def get_datasets_from_kafka(self, server_config):
        ''' Gets dataset configurations from looking at metadata available
        in all Kafka Topics.

        :param server_config: The configuration for the server.
        :type server_config: dictionary

        '''
        self.logger.info('Looking for new topics to auto configure')
        kafka_settings = get_config().get('kafka')
        kafka_url = kafka_settings.get('bootstrap_servers')
        consumer = KafkaConsumer(bootstrap_servers=[kafka_url])
        topics = consumer.topics()
        consumer.close()
        datasets = []
        existing_datasets = [
            i.title for i in self.dataset_managers if hasattr(i, 'title')]
        for topic in topics:
            if topic in self.ignored_topics:
                continue
            if topic in existing_datasets:
                self.logger.debug(
                    'Dataset for topic {0} already exists.'.format(topic))
                continue
            self.logger.info('Creating dataset for topic {0}.'.format(topic))
            consumer = KafkaConsumer(
                auto_offset_reset='earliest',
                **kafka_settings)
            dataset = self.get_dataset_from_topic(
                consumer, topic, server_config)
            consumer.close()
            if dataset:
                self.logger.info('Dataset {0} created.'.format(topic))
                datasets.append(dataset)
            else:
                self.logger.info(
                    'Dataset {0} failed to be created.'.format(topic))
                self.ignored_topics.append(topic)
        return datasets

    def get_dataset_from_topic(self, consumer, topic, server_config):
        ''' Gets dataset configurations from looking at metadata available
        in all Kafka Topics.

        :param consumer: A KafkaConsumer attached to a Kafka Instance
        :type consumer: KafkaConsumer
        :param topic: The name of the topic
        :type topic: string
        :param server_config: The configuration for the server.
        :type server_config: dictionary

        '''

        try:
            consumer.subscribe(topic)
            consumer.poll(timeout_ms=100)
            consumer.seek_to_beginning()
            poll_result = consumer.poll_and_deserialize(
                timeout_ms=1000,
                max_records=1)
            for parition_key, packages in poll_result.items():
                for package in packages:
                    schema = package.get('schema')
                    if not schema:
                        raise AttributeError('Topic %s has no schema.' % topic)
                    self.logger.info("Schema: %s" % schema)

        except NoOffsetForPartitionError as nofpe:
            self.logger.error(
                "Error on dataset creation for topic {0}; {1}".format(
                    topic,
                    nofpe))
            return None
        except AttributeError as aer:
            self.logger.error(
                "Error on dataset creation for topic {0}; {1}".format(
                    topic,
                    aer))
            return None
        safe_name = re.sub(r'\W+', '', topic).lower()
        tmp = {
            "metadata": {
                "title": topic,
                "name": safe_name,
                "owner_org": server_config.get('autoconfig_owner_org'),
                "notes": None,
                "author": None
            },
            "resources": [
                {
                    "metadata": {
                        "title": topic,
                        "description": None,
                        "name": safe_name+"-resource"
                    },
                    "topics": [
                        {
                            "name": topic,
                            "number_of_consumers": 1
                        }
                    ]
                }
            ]
        }
        return tmp

    def spawn_dataset_managers(self, server_config):
        ''' Spawns Server Managers based on the config.

        :param server_config: The configuration for the server.
        :type server_config: dictionary

        '''
        auto_config = server_config.get('autoconfig_datasets')

        if auto_config:
            if not self.autoconfig_watcher:
                self.autoconfig_watcher = AutoconfigWatcher(
                    self, server_config)
                self.autoconfig_watcher.start()
                # Delegating to the threaded / repeatable process
                return
            else:
                datasets = self.get_datasets_from_kafka(server_config)

        else:
            datasets = server_config.get('datasets')

        new_dataset_managers = []

        for dataset in datasets:
            config = {
                'ckan_url': server_config.get('url'),
                'server_name': server_config.get('title'),
                'api_key': server_config.get('api_key'),
                'dataset': dataset,
            }
            dataset_manager = DatasetManager(self, config)
            new_dataset_managers.append(dataset_manager)
            self.dataset_managers.append(dataset_manager)

        if len(new_dataset_managers) == 0:
            self.logger.info('No new Dataset Managers spawned.')
        else:
            self.logger.info(
                'Spawned {0} Dataset manager(s) for server {1}.'
                .format(len(new_dataset_managers), server_config.get('title'))
            )

        for dataset_manager in new_dataset_managers:
            dataset_manager.start()

    def stop(self):
        if self.autoconfig_watcher:
            self.autoconfig_watcher.stop()
        for dataset_manager in self.dataset_managers:
            dataset_manager.stop()

    def on_dataset_exit(self, dataset_name):
        for dataset_manager in self.dataset_managers:
            if dataset_manager.name == dataset_name:
                self.dataset_managers.remove(dataset_manager)
                break

        if len(self.dataset_managers) == 0:
            self.process_manager.on_server_exit(self.server_config.get('url'))


class AutoconfigWatcher(Thread):
    def __init__(self, server_manager, server_config):
        super(AutoconfigWatcher, self).__init__()
        self.stopped = False
        self.server_manager = server_manager
        self.server_config = server_config

    def run(self):
        while not self.stopped:
            self.server_manager.spawn_dataset_managers(self.server_config)
            for tick in range(30):
                sleep(1)
                if self.stopped:
                    return

    def stop(self):
        self.stopped = True
