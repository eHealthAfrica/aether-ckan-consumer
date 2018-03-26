from time import sleep
import signal
import sys
import logging


class ProcessManager(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def run(self):
        self.listen_stop_signal()

        while True:
            self.logger.info('Working...')
            sleep(1)

    def on_stop_handler(self, signum, frame):
        self.logger.info('Gracefully stopping...')
        sys.exit(0)

    def listen_stop_signal(self):
        signal.signal(signal.SIGINT, self.on_stop_handler)
        signal.signal(signal.SIGTERM, self.on_stop_handler)
