import logging
import os
import time
import unittest

from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.messager import Messager
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.ingest.icemetadatasourceservice import IceMetadataSourceService
from jacalingest.ingest.tosmetadata import TOSMetadata
from jacalingest.stringdomain.tostringservice import ToStringService
from jacalingest.stringdomain.stringmessage import StringMessage
from jacalingest.stringdomain.stringwriterservice import StringWriterService
from jacalingest.testbed.icerunner import IceRunner
from jacalingest.testbed.playback import Playback

class TestIceMetadataSourceService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')
    

    def test(self):
        messaging_system = QueueMessagingSystem()
        messager = Messager()

        metadata_endpoint = messager.get_endpoint(messaging_system, "metadata", TOSMetadata)
        metadata_control_endpoint = messager.get_endpoint(messaging_system, " metadatacontrol", StringMessage)

        metadata_string_endpoint = messager.get_endpoint(messaging_system, "metadatastring", StringMessage)
        tostring_control_endpoint = messager.get_endpoint(messaging_system, "tostringcontrol", StringMessage)
        string_writer_control_endpoint = messager.get_endpoint(messaging_system, "stringwritercontrol", StringMessage)
        
        # start Ice
        logging.info("Starting Ice")
        ice_runner = IceRunner("testbed_data")
        ice_runner.start()

        # set up the ice metadata source service
        ice_metadata_source_service = IceMetadataSourceService("localhost", 4061, "IceStorm/TopicManager@IceStorm.TopicManager", "metadata", "IngestPipeline", metadata_endpoint, metadata_control_endpoint)
        ice_metadata_source_service_container = ServiceContainer(ice_metadata_source_service, messager)

        tostring_service = ToStringService(metadata_endpoint, metadata_string_endpoint, tostring_control_endpoint)
        tostring_service_container = ServiceContainer(tostring_service, messager)

        string_writer_service = StringWriterService(metadata_string_endpoint, string_writer_control_endpoint, "output.txt")
        string_writer_service_container = ServiceContainer(string_writer_service, messager)


        logging.info("Starting services")
        string_writer_service_container.start()
        tostring_service_container.start()
        ice_metadata_source_service_container.start()
    
        logging.info("Starting processing")
        messager.publish(string_writer_control_endpoint, StringMessage("Start"))
        messager.publish(tostring_control_endpoint, StringMessage("Start"))
        messager.publish(metadata_control_endpoint, StringMessage("Start"))
    
        # start playback
        logging.info("Starting playback")
        playback = Playback("testbed_data")
        playback.playback("data/ade1card.ms")
        playback.wait()

        time.sleep(10)
        messager.publish(metadata_control_endpoint, StringMessage("Stop"))

        time.sleep(10)
        messager.publish(tostring_control_endpoint, StringMessage("Stop"))

        time.sleep(10)
        messager.publish(string_writer_control_endpoint, StringMessage("Stop"))

        time.sleep(10)
        ice_metadata_source_service_container.terminate()
        tostring_service_container.terminate()
        string_writer_service_container.terminate()

        # stop Ice
        logging.info("stopping Ice")
        ice_runner.stop()



if __name__ == '__main__':
    unittest.main()

