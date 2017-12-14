import logging
import time
import unittest

from jacalingest.engine.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.service import Service
from jacalingest.ingest.visibilitydatagramsourceservice import VisibilityDatagramSourceService
from jacalingest.ingest.visibilitydatagram import VisibilityDatagram
from jacalingest.stringdomain.tostringservice import ToStringService
from jacalingest.stringdomain.stringwriterservice import StringWriterService
from jacalingest.testbed.icerunner import IceRunner
from jacalingest.testbed.playback import Playback

class TestVisibilityDatagramSourceService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')




    def test(self):
        queue_messaging_system = QueueMessagingSystem()
        messaging_context = {"DATA": (queue_messaging_system, Service.WHEN_PROCESSING),
                             "M&C": (queue_messaging_system, Service.ALWAYS)}

        # set up the visibility datagram source service
        visibility_datagram_source_service = VisibilityDatagramSourceService("localhost", 3000, messaging_context, "visibility", "DATA", "playbackstatus", "visibilitystatus", "M&C", terminate=True)

        to_string_service = ToStringService(messaging_context, VisibilityDatagram, "visibility", "visibilitystring", "DATA", "visibilitystatus", "tostringstatus", "M&C", terminate=True)
    
        string_writer_service = StringWriterService(messaging_context, "visibilitystring", "DATA", "tostringstatus", "M&C", "output.txt", terminate=True)
    
        # start Ice
        logging.info("Starting Ice")
        ice_runner = IceRunner("testbed_data")
        ice_runner.start()

        logging.info("Starting services")
        string_writer_service.start()
        to_string_service.start()
        visibility_datagram_source_service.start()
    
        logging.info("Starting processing")
        string_writer_service.start_processing()
        to_string_service.start_processing()
        visibility_datagram_source_service.start_processing()
    
        # start playback
        logging.info("Starting playback")
        playback = Playback("testbed_data")
        playback.playback("data/ade1card.ms")
        playback.wait()

        logging.info("Playback has finished; sending drain message.")
        queue_messaging_system.publish("playbackstatus", "Finished")

        logging.info("Waiting for VisibilityDatagramSourceService to finish...")
        visibility_datagram_source_service.wait()

        logging.info("Waiting for ToStringService to finish...")
        to_string_service.wait()

        logging.info("Waiting for StringWriterService to finish...")
        string_writer_service.wait()

    
        # stop Ice
        logging.info("stopping Ice")
        ice_runner.stop()


if __name__ == '__main__':
    unittest.main()

