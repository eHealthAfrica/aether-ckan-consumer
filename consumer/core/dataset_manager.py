import logging
from threading import Thread

from consumer.core.topic_manager import TopicManager


class DatasetManager(Thread):

    def __init__(self, config):
        super(DatasetManager, self).__init__()

        self.logger = logging.getLogger(__name__)
        self.config = config

    def run(self):
        self.spawn_topic_managers()

    def spawn_topic_managers(self):
        dataset = self.config.get('dataset')
        resources = dataset.get('resources')
        self.topic_managers = []

        for resource in resources:
            topics = resource.get('topics')

            for topic_config in topics:
                number_of_consumers = topic_config.get('number_of_consumers')

                for i in range(number_of_consumers):
                    config = {
                        'server_name': self.config.get('server_name'),
                        'dataset_name': dataset.get('name'),
                        'topic': topic_config,
                    }
                    topic_manager = TopicManager(config)
                    self.topic_managers.append(topic_manager)

            if len(self.topic_managers) == 0:
                self.logger.info('No Topic Managers spawned.')
            else:
                self.logger.info(
                    'Spawned {0} Topic manager(s) for dataset {1}.'
                    .format(len(self.topic_managers), dataset.get('name'))
                )

        for topic_manager in self.topic_managers:
            topic_manager.start()

    def new_message_from_topic_manager(self):
        pass

    def send_data_to_dataset(self):
        pass

    def introspect_schema_from_message(self):
        pass
