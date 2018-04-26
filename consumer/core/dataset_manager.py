import logging
from threading import Thread
import json
import sys
import os

from ckanapi import RemoteCKAN
from ckanapi import errors as ckanapi_errors

from consumer.core.resource_manager import ResourceManager
from consumer.config import parse_json_from_file


class DatasetManager(Thread):

    def __init__(self, config):
        super(DatasetManager, self).__init__()

        self.logger = logging.getLogger(__name__)
        self.config = config

    def run(self):
        self.create_dataset_in_ckan()
        self.spawn_resource_managers()

    def spawn_resource_managers(self):
        dataset = self.config.get('dataset')
        dataset_name = dataset.get('metadata').get('name')
        resources = dataset.get('resources')
        self.resource_managers = []

        for resource in resources:
            config = {
                'dataset': dataset,
                'resource': resource,
                'server_name': self.config.get('server_name'),
                'ckan_url': self.config.get('ckan_url'),
                'api_key': self.config.get('api_key'),
            }
            resource_manager = ResourceManager(config)
            self.resource_managers.append(resource_manager)

        if len(self.resource_managers) == 0:
            self.logger.info('No Resource Managers spawned.')
        else:
            self.logger.info(
                'Spawned {0} Resource manager(s) for dataset {1}.'
                .format(len(self.resource_managers), dataset_name)
            )

        for resource_manager in self.resource_managers:
            resource_manager.start()

    def create_dataset_in_ckan(self):
        server_url = self.config.get('ckan_url')
        server_title = self.config.get('server_name')
        api_key = self.config.get('api_key')
        metadata = self.config.get('dataset').get('metadata')
        dataset_name = metadata.get('name')

        ckan = RemoteCKAN(server_url, apikey=api_key)

        try:
            ckan.action.package_show(id=dataset_name)
            return
        except ckanapi_errors.NotFound:
            # Dataset does not exist, so continue with execution to create it.
            pass

        try:
            data = self.update_metadata_from_defaults(metadata)
            dataset = ckan.action.package_create(**data)
            dataset_url = '{0}/dataset/{1}'.format(server_url, dataset_name)
            self.logger.info('Dataset {0} created in CKAN portal {1}: {2}.'
                             .format(
                                dataset_name,
                                server_title,
                                dataset_url
                             ))
        except ckanapi_errors.NotAuthorized as e:
            self.logger.error(
                'Cannot create dataset {0}. {1}'.format(
                    dataset_name,
                    str(e),
                )
            )
            sys.exit(1)
        except ckanapi_errors.ValidationError as e:
            self.logger.error(
                'Cannot create dataset {0}. Payload is not valid. Check the '
                'following errors: {1}'.format(
                    dataset_name,
                    json.dumps(e.error_dict),
                )
            )
            sys.exit(1)

    def update_metadata_from_defaults(self, overwritten_data):
        dir_path = os.getcwd()
        file_name = os.path.join('config', 'dataset_metadata.json')

        default_metadata = parse_json_from_file(dir_path, file_name)

        updated_metadata = {}
        updated_metadata.update(default_metadata)
        updated_metadata.update(overwritten_data)

        return updated_metadata
