import logging
import time
import unittest

from jacalingest.stringdomain.tostringservice import ToStringService
from jacalingest.engine.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.service import Service
from jacalingest.ingest.visibilitydatagram import VisibilityDatagram

class TestToStringService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        queue_messaging_system = QueueMessagingSystem()
        messaging_context = {"DATA": (queue_messaging_system, Service.WHEN_PROCESSING),
                             "M&C": (queue_messaging_system, Service.ALWAYS)}

        to_string_service = ToStringService(messaging_context, VisibilityDatagram, "message", "string", "DATA", "in_status", "out_status", "M&C")
        to_string_service.start()
        time.sleep(5)
        to_string_service.start_processing()
        time.sleep(10)
        to_string_service.stop_processing()
        time.sleep(5)
        to_string_service.terminate()
        to_string_service.wait()

if __name__ == '__main__':
    unittest.main()

