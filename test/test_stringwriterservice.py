import logging
import time
import unittest

from jacalingest.stringdomain.stringwriterservice import StringWriterService
from jacalingest.engine.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.service import Service

class TestStringWriterService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
   
        queue_messaging_system = QueueMessagingSystem()
        messaging_context = {"DATA": (queue_messaging_system, Service.WHEN_PROCESSING),
                             "M&C": (queue_messaging_system, Service.ALWAYS)}
        string_writer_service = StringWriterService(messaging_context, "string","DATA", "status", "M&C", "output.txt")
        string_writer_service.start()
        time.sleep(5)
        string_writer_service.start_processing()
        time.sleep(10)
        string_writer_service.stop_processing()
        time.sleep(5)
        string_writer_service.terminate()
        string_writer_service.wait()

if __name__ == '__main__':
    unittest.main()

