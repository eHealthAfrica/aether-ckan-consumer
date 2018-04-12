import logging
import os

from consumer.core.process_manager import ProcessManager
from consumer.config import validate_config
from consumer import db


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info('Starting application...')

    dir_path = os.getcwd()

    # Located in the config directory
    config_file = os.path.join('config', 'config.json')
    schema_file = os.path.join('config', 'config.schema')

    validate_config(dir_path, config_file, schema_file)

    db.init()

    print db.get_session()

    processManager = ProcessManager()
    processManager.run()
