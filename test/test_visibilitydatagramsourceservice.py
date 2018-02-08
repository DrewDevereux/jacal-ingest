import logging
import time
import unittest

from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.messager import Messager
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.ingest.visibilitydatagramsourceservice import VisibilityDatagramSourceService
from jacalingest.ingest.visibilitydatagram import VisibilityDatagram
from jacalingest.stringdomain.tostringservice import ToStringService
from jacalingest.stringdomain.stringmessage import StringMessage
from jacalingest.stringdomain.stringwriterservice import StringWriterService
from jacalingest.testbed.icerunner import IceRunner
from jacalingest.testbed.playback import Playback

class TestVisibilityDatagramSourceService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')




    def test(self):
        messaging_system = QueueMessagingSystem()
        messager = Messager()

        visibility_endpoint = messager.get_endpoint(messaging_system, "visibility", VisibilityDatagram)
        visibility_control_endpoint = messager.get_endpoint(messaging_system, "visibilitycontrol", StringMessage)

        visibility_string_endpoint = messager.get_endpoint(messaging_system, "visibilitystring", StringMessage)
        tostring_control_endpoint = messager.get_endpoint(messaging_system, "tostringcontrol", StringMessage)
        string_writer_control_endpoint = messager.get_endpoint(messaging_system, "stringwritercontrol", StringMessage)

        # set up the visibility datagram source service
        visibility_datagram_source_service = VisibilityDatagramSourceService("localhost", 3000, visibility_endpoint, visibility_control_endpoint)
        visibility_datagram_source_service_container = ServiceContainer(visibility_datagram_source_service, messager)

        tostring_service = ToStringService(visibility_endpoint, visibility_string_endpoint, tostring_control_endpoint)
        tostring_service_container = ServiceContainer(tostring_service, messager)

        string_writer_service = StringWriterService(visibility_string_endpoint, string_writer_control_endpoint, "output.txt")
        string_writer_service_container = ServiceContainer(string_writer_service, messager)

        # start Ice
        logging.info("Starting Ice")
        ice_runner = IceRunner("testbed_data")
        ice_runner.start()

        logging.info("Starting services")
        string_writer_service_container.start()
        tostring_service_container.start()
        visibility_datagram_source_service_container.start()
    
        logging.info("Starting processing")
        messager.publish(string_writer_control_endpoint, StringMessage("Start"))
        messager.publish(tostring_control_endpoint, StringMessage("Start"))
        messager.publish(visibility_control_endpoint, StringMessage("Start"))

        # start playback
        logging.info("Starting playback")
        playback = Playback("testbed_data")
        playback.playback("data/ade1card.ms")
        playback.wait()

        time.sleep(10)
        messager.publish(visibility_control_endpoint, StringMessage("Stop"))

        time.sleep(10)
        messager.publish(tostring_control_endpoint, StringMessage("Stop"))

        time.sleep(10)
        messager.publish(string_writer_control_endpoint, StringMessage("Stop"))

        time.sleep(10)

        visibility_datagram_source_service_container.terminate()
        tostring_service_container.terminate()
        string_writer_service_container.terminate()

        # stop Ice
        logging.info("stopping Ice")
        ice_runner.stop()


if __name__ == '__main__':
    unittest.main()

