import logging
from threading import Thread
import sys
import json

from ckanapi import RemoteCKAN
from ckanapi import errors as ckanapi_errors

from consumer.core.topic_manager import TopicManager
from consumer.db import Resource, CkanServer


class ResourceManager(Thread):

    def __init__(self, config):
        super(ResourceManager, self).__init__()

        self.logger = logging.getLogger(__name__)
        self.config = config

    def run(self):
        resource = self.config.get('resource')
        dataset_name = self.config.get('dataset').get('metadata').get('name')

        self.create_resource_in_ckan(resource, dataset_name)
        self.create_resource_in_db(resource, dataset_name)

        self.spawn_topic_managers()

    def spawn_topic_managers(self):
        dataset = self.config.get('dataset')
        dataset_name = dataset.get('metadata').get('name')
        resource = self.config.get('resource')
        self.topic_managers = []

        topics = resource.get('topics')

        for topic_config in topics:
            number_of_consumers = topic_config.get('number_of_consumers')

            for i in range(number_of_consumers):
                config = {
                    'server_name': self.config.get('server_name'),
                    'dataset_name': dataset_name,
                    'topic': topic_config,
                }
                topic_manager = TopicManager(config)
                self.topic_managers.append(topic_manager)

        if len(self.topic_managers) == 0:
            self.logger.info('No Topic Managers spawned.')
        else:
            self.logger.info(
                'Spawned {0} Topic manager(s) for dataset {1}.'
                .format(len(self.topic_managers), dataset_name)
            )

        for topic_manager in self.topic_managers:
            topic_manager.start()

    def create_resource_in_db(self, resource, dataset_name):
        metadata = resource.get('metadata')
        resource_name = metadata.get('name')
        ckan_server_url = self.config.get('ckan_url')
        ckan_server = CkanServer.get_by_url(
            ckan_server_url=ckan_server_url
        )
        resource = Resource.get(
            resource_name=resource_name,
            ckan_server_id=ckan_server.ckan_server_id,
            dataset_name=dataset_name
        )

        if not resource:
            data = {
                'resource_name': resource_name,
                'dataset_name': dataset_name,
                'ckan_server_id': ckan_server.ckan_server_id
            }

            Resource.create(**data)

    def create_resource_in_ckan(self, resource, dataset_name):
        server_url = self.config.get('ckan_url')
        server_title = self.config.get('server_name')
        api_key = self.config.get('api_key')
        title = resource.get('metadata').get('title')
        resource_name = resource.get('metadata').get('name')
        resource_description = resource.get('metadata').get('description')

        payload = {
            'package_id': dataset_name,
            'name': title,
            'description': resource_description,
            'url_type': 'datastore',
            'url': ' ',
        }

        ckan = RemoteCKAN(server_url, apikey=api_key)

        try:
            ckan_server = CkanServer.get_by_url(
                ckan_server_url=server_url
            )
            db_resource = Resource.get(
                resource_name=resource_name,
                dataset_name=dataset_name,
                ckan_server_id=ckan_server.ckan_server_id
            )

            if db_resource:
                return
        except ckanapi_errors.NotFound:
            # Resource does not exist, so continue with execution to create it.
            pass

        try:
            response = ckan.action.resource_create(**payload)
            resource_url = '{0}/dataset/{1}/resource/{2}'.format(
                server_url, dataset_name, response.get('id')
            )
            self.logger.info('Resource {0} created in CKAN portal {1}: {2}.'
                             .format(
                                resource_name,
                                server_title,
                                resource_url
                             ))
        except ckanapi_errors.NotAuthorized as e:
            self.logger.error(
                'Cannot create resource {0}. {1}'.format(
                    resource_name,
                    str(e),
                )
            )
            sys.exit(1)
        except ckanapi_errors.ValidationError as e:
            self.logger.error(
                'Cannot create resource {0}. Payload is not valid. Check the '
                'following errors: {1}'.format(
                    resource_name,
                    json.dumps(e.error_dict),
                )
            )
            sys.exit(1)
