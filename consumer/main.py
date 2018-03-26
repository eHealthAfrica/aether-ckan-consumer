import logging

from process_manager import ProcessManager


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info('Starting application...')

    processManager = ProcessManager()
    processManager.run()
