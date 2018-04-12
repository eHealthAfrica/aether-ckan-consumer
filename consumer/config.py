import os
import json
import logging
import sys

from jsonschema import validate
from jsonschema.exceptions import ValidationError


logger = logging.getLogger(__name__)
config = None


def read_file(dir_path, file_name):
    ''' Read file from local filesystem.

    :param dir_path: The directory path where the file is located.
    :type dir_path: string

    :param file_name: The name of the file to read from.
    :type file_name: string

    :raises SystemExit: If the file cannot be found from provided directory
    path and file name.

    :returns: The contents of the file.
    :rtype: string

    '''

    file_path = os.path.join(dir_path, file_name)

    try:
        with open(file_path, 'r') as f:
            return f.read()
    except IOError:
        logger.error('{0} does not exist in directory {1}.'
                     .format(file_name, dir_path))
        sys.exit(1)


def parse_json_from_file(dir_path, file_name):
    ''' Parses JSON file from local filesystem.

    :param dir_path: The directory path where the file is located.
    :type dir_path: string

    :param file_name: The name of the file to read from.
    :type file_name: string

    :raises SystemExit: If the file cannot be parsed as JSON.

    :returns: The contents of the file as JSON.
    :rtype: dictionary

    '''

    content = read_file(dir_path, file_name)

    try:
        return json.loads(content)
    except (ValueError, TypeError):
        logger.error('{0} is not a valid JSON file.'
                     .format(file_name))
        sys.exit(1)


def validate_config(dir_path, config_file, schema_file):
    ''' Validates the config file if it conforms to JSON Schema.

    :param dir_path: The directory path where the file is located.
    :type dir_path: string

    :param config_file: The config file to validate.
    :type config_file: string

    :param schema_file: The JSON Schema file to use for validation.
    :type schema_file: string

    :raises SystemExit: If the config file is not valid according to
    the JSON Schema file.

    '''

    config_file = parse_json_from_file(dir_path, config_file)
    schema_file = parse_json_from_file(dir_path, schema_file)

    try:
        validate(config_file, schema_file)
        global config
        config = config_file
    except ValidationError as e:
        logger.error('Error while validating config.json. '
                     'Please fix the following error:')
        logger.error(e)
        sys.exit(1)


def get_config():
    return config
