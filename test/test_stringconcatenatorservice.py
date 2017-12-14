import logging
import time
import unittest

from jacalingest.stringdomain.stringconcatenatorservice import StringConcatenatorService
from jacalingest.engine.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.service import Service

class TestStringConcatenatorService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        messaging_context={"ALL": (QueueMessagingSystem(), Service.WHEN_PROCESSING)}
        string_concatenator_service = StringConcatenatorService(messaging_context, "first", "second", "concatenation", "ALL")
        string_concatenator_service.start()
        time.sleep(5)
        string_concatenator_service.start_processing()
        time.sleep(10)
        string_concatenator_service.stop_processing()
        time.sleep(5)
        string_concatenator_service.terminate()
        string_concatenator_service.wait()

if __name__ == '__main__':
    unittest.main()

