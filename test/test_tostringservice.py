import logging
import time
import unittest

from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.messaging.messager import Messager
from jacalingest.ingest.visibilitydatagram import VisibilityDatagram
from jacalingest.stringdomain.tostringservice import ToStringService
from jacalingest.stringdomain.stringmessage import StringMessage

class TestToStringService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        messaging_system = QueueMessagingSystem()
        messager = Messager()
        datagram_endpoint = messager.get_endpoint(messaging_system, "datagram", VisibilityDatagram)
        string_endpoint = messager.get_endpoint(messaging_system, "string", StringMessage)
        control_endpoint = messager.get_endpoint(messaging_system, "control", StringMessage)

        to_string_service = ToStringService(datagram_endpoint, string_endpoint, control_endpoint)
        to_string_service_container = ServiceContainer(to_string_service, messager)

        to_string_service_container.start()
        time.sleep(5)
        messager.publish(control_endpoint, StringMessage("Start"))
        time.sleep(10)
        messager.publish(control_endpoint, StringMessage("Stop"))
        time.sleep(5)
        to_string_service_container.terminate()
        to_string_service_container.wait()

if __name__ == '__main__':
    unittest.main()

