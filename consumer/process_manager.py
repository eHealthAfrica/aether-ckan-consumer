from time import sleep
import signal
import sys


class ProcessManager(object):
    def __init__(self):
        pass

    def run(self):
        self.listen_stop_signal()

        while True:
            print 'Working..'
            sleep(1)

    def on_stop_handler(self, signum, frame):
        print 'Exiting application...'
        sys.exit(0)

    def listen_stop_signal(self):
        signal.signal(signal.SIGINT, self.on_stop_handler)
        signal.signal(signal.SIGTERM, self.on_stop_handler)
