from time import sleep
import signal
import sys
import logging
import os
import json

from jsonschema import validate
from jsonschema.exceptions import ValidationError


PROCESS_ACTIVE_TIMEOUT = 1


class ProcessManager(object):
    ''' Responsible for managing the internal lifecycle of the application. '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def run(self):
        ''' Spawns Server Managers and keeps the main process active. '''

        self.listen_stop_signal()

        dir_path = os.getcwd()
        config_file = 'config.json'
        schema_file = 'config.schema'

        config = self.validate_config(dir_path, config_file, schema_file)

        # This is a method that blocks the main thread where the application
        # was started. It must always be at the bottom of the run() method.
        self.keep_process_active()

    def keep_process_active(self):
        ''' Keeps the process active until stop signal is caught which
        will kill the process and stop the application. '''

        while True:
            sleep(PROCESS_ACTIVE_TIMEOUT)

    def on_stop_handler(self, signum, frame):
        ''' Called when the application needs to be gracefully stopped. '''

        self.logger.info('Gracefully stopping...')
        sys.exit(0)

    def listen_stop_signal(self):
        ''' Listens for the SIGTERM signal so that the application can be
        gracefully stopped. '''

        # Catches Ctrl+C and docker stop
        signal.signal(signal.SIGTERM, self.on_stop_handler)

    def read_file(self, dir_path, file_name):
        ''' Read file from local filesystem.

        :param dir_path: The directory path where the file is located.
        :type dir_path: string

        :param file_name: The name of the file to read from.
        :type file_name: string

        :raises IOError: If the file cannot be found from provided directory
        path and file name.

        :returns: The contents of the file.
        :rtype: string

        '''

        file_path = os.path.join(dir_path, file_name)

        try:
            with open(file_path, 'r') as f:
                return f.read()
        except IOError:
            self.logger.error('{0} does not exist in directory {1}.'
                              .format(file_name, dir_path))
            sys.exit(1)

    def parse_json_from_file(self, dir_path, file_name):
        ''' Parses JSON file from local filesystem.

        :param dir_path: The directory path where the file is located.
        :type dir_path: string

        :param file_name: The name of the file to read from.
        :type file_name: string

        :raises ValueError, TypeError: If the file cannot be parsed as JSON.

        '''

        content = self.read_file(dir_path, file_name)

        try:
            return json.loads(content)
        except (ValueError, TypeError):
            self.logger.error('{0} is not a valid JSON file.'
                              .format(file_name))
            sys.exit(1)

    def validate_config(self, dir_path, config_file, schema_file):
        ''' Validates the config file if it conforms to JSON Schema.

        :param dir_path: The directory path where the file is located.
        :type dir_path: string

        :param config_file: The config file to validate.
        :type config_file: string

        :param schema_file: The JSON Schema file to use for validation.
        :type schema_file: string

        :raises ValidationError: If the config file is not valid according to
        the JSON Schema file.

        :returns: The config file as JSON.
        :rtype: dictionary

        '''

        config = self.parse_json_from_file(dir_path, config_file)
        schema = self.parse_json_from_file(dir_path, schema_file)

        try:
            validate(config, schema)
        except ValidationError as e:
            self.logger.error('Error while validating config.json. '
                              'Please fix the following error:')
            self.logger.error(e)
            sys.exit(1)

        return config
