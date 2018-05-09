import unittest

import pook

from consumer.core.server_manager import ServerManager
from consumer.core.dataset_manager import DatasetManager


class TestServerManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestServerManager, self).__init__(*args, **kwargs)

        config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        self.server_manager = ServerManager(None, config)

    @pook.activate
    def test_check_server_availability(self):
        pook.get('http://ckan-server1.com/api/action/status_show') \
            .reply(200) \
            .json({'success': True})

        server_config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        server_available = self.server_manager.check_server_availability(
            server_config
        )

        assert server_available is True

    @pook.activate
    def test_check_server_availability_with_404(self):
        pook.get('http://ckan-server1.com/api/action/status_show') \
            .reply(404) \
            .json({'success': True})

        server_config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        server_available = self.server_manager.check_server_availability(
            server_config
        )

        assert server_available is False

    @pook.activate
    def test_check_server_availability_with_no_success(self):
        pook.get('http://ckan-server1.com/api/action/status_show') \
            .reply(404) \
            .json({'success': False})

        server_config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        server_available = self.server_manager.check_server_availability(
            server_config
        )

        assert server_available is False

    @pook.activate
    def test_check_server_availability_with_no_json(self):
        pook.get('http://ckan-server1.com/api/action/status_show') \
            .reply(404) \
            .body('i am a body')

        server_config = {
            'url': 'http://ckan-server1.com',
            'datasets': []
        }

        server_available = self.server_manager.check_server_availability(
            server_config
        )

        assert server_available is False

    @pook.activate
    def test_spawn_dataset_managers(self):
        data = {
            'error': {
                'message': 'Not found',
                '__type': 'Not Found Error'
            }
        }

        pook.post('http://ckan-server1.com/api/action/package_show') \
            .reply(200) \
            .json({'success': True, 'result': {}})

        config = {
            'url': 'http://ckan-server1.com',
            'datasets': [{
                'metadata': {
                    'title': 'Some title',
                    'name': 'Some name',
                    'notes': 'Some description',
                },
                'resources': [],
            }]
        }

        self.server_manager.spawn_dataset_managers(config)

        assert len(self.server_manager.dataset_managers) == 1
        assert type(self.server_manager.dataset_managers[0]) is DatasetManager
