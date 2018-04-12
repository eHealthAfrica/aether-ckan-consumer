import unittest

from mock import Mock
import responses

from consumer.core.process_manager import ProcessManager
from consumer.core.server_manager import ServerManager


class TestProcessManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestProcessManager, self).__init__(*args, **kwargs)

        self.process_manager = ProcessManager()

    def test_run(self):
        self.process_manager.listen_stop_signal = Mock()
        self.process_manager.spawn_server_managers = Mock()
        self.process_manager.run()

        assert self.process_manager.listen_stop_signal.called
        assert self.process_manager.spawn_server_managers.called

    @responses.activate
    def test_spawn_server_managers(self):
        responses.add(
            responses.GET,
            'http://ckan-server1.com/api/action/status_show',
            json={'success': True},
            status=200
        )

        responses.add(
            responses.GET,
            'http://ckan-server2.com/api/action/status_show',
            json={'success': True},
            status=200
        )

        config = {
            'ckan_servers': [
                {'url': 'http://ckan-server1.com', 'datasets': []},
                {'url': 'http://ckan-server2.com', 'datasets': []}
            ]
        }

        self.process_manager.spawn_server_managers(config)

        assert len(self.process_manager.server_managers) == \
            len(config.get('ckan_servers'))
        assert type(self.process_manager.server_managers[0]) is ServerManager
        assert type(self.process_manager.server_managers[1]) is ServerManager

    @responses.activate
    def test_spawn_server_managers_with_404(self):
        responses.add(
            responses.GET,
            'http://ckan-server1.com/api/action/status_show',
            json={'success': True},
            status=200
        )

        responses.add(
            responses.GET,
            'http://ckan-server2.com/api/action/status_show',
            json={'success': True},
            status=404
        )

        config = {
            'ckan_servers': [
                {'url': 'http://ckan-server1.com', 'datasets': []},
                {'url': 'http://ckan-server2.com', 'datasets': []}
            ]
        }

        self.process_manager.spawn_server_managers(config)

        assert len(self.process_manager.server_managers) == 1
        assert type(self.process_manager.server_managers[0]) is ServerManager
