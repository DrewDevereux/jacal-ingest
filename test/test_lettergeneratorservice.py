import logging
import time
import unittest

from jacalingest.stringdomain.lettergeneratorservice import LetterGeneratorService
from jacalingest.engine.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.service import Service

class TestLetterGeneratorService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        messaging_context = {"ALL": (QueueMessagingSystem(), Service.WHEN_PROCESSING)}

        letter_generator_service = LetterGeneratorService(messaging_context, "letters", "ALL")
        letter_generator_service.start()
        time.sleep(5)
        letter_generator_service.start_processing()
        time.sleep(10)
        letter_generator_service.stop_processing()
        time.sleep(5)
        letter_generator_service.terminate()
        letter_generator_service.wait()

if __name__ == '__main__':
    unittest.main()

