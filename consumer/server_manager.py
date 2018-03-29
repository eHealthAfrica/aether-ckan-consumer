from time import sleep
import logging

import requests
from requests.exceptions import ConnectionError

from dataset_manager import DatasetManager


CONN_RETRY = 3
CONN_RETRY_WAIT_TIME = 1


class ServerManager(object):

    def __init__(self, server):
        self.logger = logging.getLogger(__name__)
        self.server = server

        server_available = self.check_server_availability()

        if server_available:
            self.spawn_dataset_managers(self.server)

    def check_server_availability(self):
        ''' Checks the server availability using CKAN's API action
        "status_show". Retry logic exist with a wait time between retries.

        :returns: Is server available.
        :rtype: boolean

        '''

        server_url = self.server.get('url')
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

    def spawn_dataset_managers(self, server):
        ''' Spawns Server Managers based on the config. '''

        datasets = server.get('datasets')
        self.dataset_managers = []

        for dataset in datasets:
            dataset_manager = DatasetManager(dataset)
            self.dataset_managers.append(dataset_manager)
