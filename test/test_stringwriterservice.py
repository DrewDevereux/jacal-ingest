import logging
import time
import unittest

from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.messager import Messager
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.stringdomain.stringmessage import StringMessage
from jacalingest.stringdomain.stringwriterservice import StringWriterService

class TestStringWriterService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
   
        messaging_system = QueueMessagingSystem()
        messager = Messager()
        string_endpoint = messager.get_endpoint(messaging_system, "string", StringMessage)
        control_endpoint = messager.get_endpoint(messaging_system, "control", StringMessage)


        string_writer_service = StringWriterService(string_endpoint, control_endpoint, "output.txt")
        string_writer_service_container = ServiceContainer(string_writer_service, messager)

        string_writer_service_container.start()
        time.sleep(5)
        messager.publish(control_endpoint, StringMessage("Start"))
        time.sleep(10)
        messager.publish(control_endpoint, StringMessage("Stop"))
        time.sleep(5)
        string_writer_service_container.terminate()
        string_writer_service_container.wait()

if __name__ == '__main__':
    unittest.main()

