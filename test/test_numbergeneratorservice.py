import logging
import time
import unittest

from jacalingest.stringdomain.numbergeneratorservice import NumberGeneratorService
from jacalingest.engine.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.service import Service

class TestNumberGeneratorService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        messaging_context = {"ALL": (QueueMessagingSystem(), Service.WHEN_PROCESSING)}
        number_generator_service = NumberGeneratorService(messaging_context, "numbers", "ALL")
        number_generator_service.start()
        time.sleep(5)
        number_generator_service.start_processing()
        time.sleep(10)
        number_generator_service.stop_processing()
        time.sleep(5)
        number_generator_service.terminate()
        number_generator_service.wait()

if __name__ == '__main__':
    unittest.main()

