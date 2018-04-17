import signal
import sys
import logging

from consumer.core.server_manager import ServerManager
from consumer.config import get_config
from consumer.db import CkanServer


class ProcessManager(object):
    ''' Responsible for managing the internal lifecycle of the application. '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def run(self):
        self.listen_stop_signal()
        config = get_config()
        self.spawn_server_managers(config)

    def on_stop_handler(self, signum, frame):
        ''' Called when the application needs to be gracefully stopped. '''

        self.logger.info('Gracefully stopping...')
        sys.exit(0)

    def listen_stop_signal(self):
        ''' Listens for the SIGTERM signal so that the application can be
        gracefully stopped. '''

        # Catches Ctrl+C and docker stop
        signal.signal(signal.SIGTERM, self.on_stop_handler)

    def spawn_server_managers(self, config):
        ''' Spawns Server Managers based on the config.

        :param config: Configuration based on config.json.
        :type config: dictionary

        '''

        servers = config.get('ckan_servers')
        self.server_managers = []

        for server_config in servers:
            server_manager = ServerManager(server_config)

            server_available = server_manager.check_server_availability(
                server_config
            )

            if server_available:
                self.server_managers.append(server_manager)
                server_manager.spawn_dataset_managers(server_config)

                ckan_server_url = server_config.get('url')
                ckan_server = CkanServer.get_by_url(
                    ckan_server_url=ckan_server_url
                )

                if not ckan_server:
                    CkanServer.create(ckan_server_url=ckan_server_url)

        if len(self.server_managers) == 0:
            self.logger.error('No CKAN servers available.')
            sys.exit(1)
        else:
            self.logger.info('Spawned {0} Server manager(s).'
                             .format(len(self.server_managers)))
