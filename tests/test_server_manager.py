import unittest

import responses

from consumer.core.server_manager import ServerManager
from consumer.core.dataset_manager import DatasetManager


class TestServerManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestServerManager, self).__init__(*args, **kwargs)

        config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        self.server_manager = ServerManager(config)

    @responses.activate
    def test_check_server_availability(self):
        responses.add(
            responses.GET,
            'http://ckan-server1.com/api/action/status_show',
            json={'success': True},
            status=200
        )

        server_config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        server_available = self.server_manager.check_server_availability(
            server_config
        )

        assert server_available is True

    @responses.activate
    def test_check_server_availability_with_404(self):
        responses.add(
            responses.GET,
            'http://ckan-server1.com/api/action/status_show',
            json={'success': True},
            status=404
        )

        server_config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        server_available = self.server_manager.check_server_availability(
            server_config
        )

        assert server_available is False

    @responses.activate
    def test_check_server_availability_with_no_success(self):
        responses.add(
            responses.GET,
            'http://ckan-server1.com/api/action/status_show',
            json={'success': False},
            status=404
        )

        server_config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        server_available = self.server_manager.check_server_availability(
            server_config
        )

        assert server_available is False

    @responses.activate
    def test_check_server_availability_with_no_json(self):
        responses.add(
            responses.GET,
            'http://ckan-server1.com/api/action/status_show',
            body='i am a body',
            status=404
        )

        server_config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        server_available = self.server_manager.check_server_availability(
            server_config
        )

        assert server_available is False

    def test_spawn_dataset_managers(self):
        config = {
            'datasets': [{
                'resources': []
            }]
        }

        self.server_manager.spawn_dataset_managers(config)

        assert len(self.server_manager.dataset_managers) == 1
        assert type(self.server_manager.dataset_managers[0]) is DatasetManager
