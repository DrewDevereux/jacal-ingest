import logging
import time
import unittest

from jacalingest.stringdomain.stringmessage import StringMessage
from jacalingest.stringdomain.numbergeneratorservice import NumberGeneratorService
from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.messaging.messager import Messager

class TestNumberGeneratorService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        messaging_system = QueueMessagingSystem()
        messager = Messager()
        numbers_endpoint = messager.get_endpoint(messaging_system, "numbers", StringMessage)

        number_generator_service = NumberGeneratorService(numbers_endpoint)
        number_generator_service_container = ServiceContainer(number_generator_service, messager)
      
        number_generator_service_container.start()
        time.sleep(10)
        number_generator_service_container.terminate()
        number_generator_service_container.wait()

if __name__ == '__main__':
    unittest.main()

