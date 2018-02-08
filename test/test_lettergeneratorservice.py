import logging
import time
import unittest

from jacalingest.stringdomain.lettergeneratorservice import LetterGeneratorService
from jacalingest.stringdomain.stringmessage import StringMessage
from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.messaging.messager import Messager

class TestLetterGeneratorService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        messaging_system = QueueMessagingSystem()
        messager = Messager()
        letters_endpoint = messager.get_endpoint(messaging_system, "letters", StringMessage)

        letter_generator_service = LetterGeneratorService(letters_endpoint)
        letter_generator_service_container = ServiceContainer(letter_generator_service, messager)

        letter_generator_service_container.start()
        time.sleep(10)
        letter_generator_service_container.terminate()
        letter_generator_service_container.wait()

if __name__ == '__main__':
    unittest.main()

