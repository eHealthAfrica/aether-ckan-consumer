from time import sleep
import logging
import sys

import requests
from requests.exceptions import ConnectionError
from ckanapi import RemoteCKAN
from ckanapi import errors as ckanapi_errors

from consumer.core.dataset_manager import DatasetManager


CONN_RETRY = 3
CONN_RETRY_WAIT_TIME = 2


class ServerManager(object):

    def __init__(self, server_config):
        self.logger = logging.getLogger(__name__)
        self.server_config = server_config

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

    def spawn_dataset_managers(self, server_config):
        ''' Spawns Server Managers based on the config.

        :param server_config: The configuration for the server.
        :type server_config: dictionary

        '''

        datasets = server_config.get('datasets')
        self.dataset_managers = []

        for dataset in datasets:
            config = {
                'ckan_url': server_config.get('url'),
                'server_name': server_config.get('title'),
                'api_key': server_config.get('api_key'),
                'dataset': dataset,
            }
            dataset_manager = DatasetManager(config)
            self.dataset_managers.append(dataset_manager)
            self.create_dataset_in_ckan(server_config, dataset)

        if len(self.dataset_managers) == 0:
            self.logger.info('No Dataset Managers spawned.')
        else:
            self.logger.info(
                'Spawned {0} Dataset manager(s) for server {1}.'
                .format(len(self.dataset_managers), server_config.get('name'))
            )

        for dataset_manager in self.dataset_managers:
            dataset_manager.start()

    def create_dataset_in_ckan(self, server_config, dataset_config):
        server_url = server_config.get('url')
        server_title = server_config.get('title')
        api_key = server_config.get('api_key')
        metadata = dataset_config.get('metadata')
        dataset_name = metadata.get('name')
        data = {
            'title': metadata.get('title'),
            'name': dataset_name,
            'description': metadata.get('description'),
            'owner_org': metadata.get('owner_org'),
        }

        ckan = RemoteCKAN(server_url, apikey=api_key)

        try:
            ckan.action.package_show(id=data.get('name'))
            return
        except ckanapi_errors.NotFound:
            # Dataset does not exist, so continue with execution to create it.
            pass

        try:
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
