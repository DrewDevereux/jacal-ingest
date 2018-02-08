import logging
import os
import time
import unittest

from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.messager import Messager
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.ingest.visibilitydatagram import VisibilityDatagram
from jacalingest.ingest.visibilitydatagramsourceservice import VisibilityDatagramSourceService
from jacalingest.ingest.icemetadatasourceservice import IceMetadataSourceService
from jacalingest.ingest.tosmetadata import TOSMetadata
from jacalingest.ingest.visibilitychunk import VisibilityChunk
from jacalingest.ingest.alignservice import AlignService
from jacalingest.monitoringandcontrol.genericmonitorservice import GenericMonitorService
from jacalingest.monitoringandcontrol.metrics import Metrics
from jacalingest.monitoringandcontrol.monitoradapter import MonitorAdapter
from jacalingest.stringdomain.stringmessage import StringMessage
from jacalingest.stringdomain.stringwriterservice import StringWriterService
from jacalingest.stringdomain.tostringservice import ToStringService
from jacalingest.testbed.icerunner import IceRunner
from jacalingest.testbed.playback import Playback

class TestAlignService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')


    def test(self):
        messaging_system = QueueMessagingSystem()
        messager = Messager()

        metadata_endpoint = messager.get_endpoint(messaging_system, "metadata", TOSMetadata)
        metadata_control_endpoint = messager.get_endpoint(messaging_system, "metadatacontrol", StringMessage)
        metadata_metrics_endpoint = messager.get_endpoint(messaging_system, "metadatametrics", Metrics)

        datagram_endpoint = messager.get_endpoint(messaging_system, "datagram", VisibilityDatagram)
        datagram_control_endpoint = messager.get_endpoint(messaging_system, "datagramcontrol", StringMessage)
        datagram_metrics_endpoint = messager.get_endpoint(messaging_system, "datagrammetrics", Metrics)

        chunk_endpoint = messager.get_endpoint(messaging_system, "chunk", VisibilityChunk)
        align_control_endpoint = messager.get_endpoint(messaging_system, "aligncontrol", StringMessage)
        align_metrics_endpoint = messager.get_endpoint(messaging_system, "alignmetrics", Metrics)

        chunk_string_endpoint = messager.get_endpoint(messaging_system, "chunkstring", StringMessage)
        to_string_control_endpoint = messager.get_endpoint(messaging_system, "to_stringcontrol", StringMessage)
        to_string_metrics_endpoint = messager.get_endpoint(messaging_system, "to_stringmetrics", Metrics)
        string_writer_control_endpoint = messager.get_endpoint(messaging_system, "stringwritercontrol", StringMessage)
        string_writer_metrics_endpoint = messager.get_endpoint(messaging_system, "stringwritermetrics", Metrics)

        # start Ice
        logging.info("Starting Ice")
        ice_runner = IceRunner("testbed_data")
        ice_runner.start()
    
        # set up the ice metadata source service
        ice_metadata_source_service = IceMetadataSourceService("localhost", 4061, "IceStorm/TopicManager@IceStorm.TopicManager", "metadata", "IngestPipeline", metadata_endpoint, metadata_control_endpoint)
        ice_metadata_source_monitor_adapter = MonitorAdapter(ice_metadata_source_service, "ice_metadata_source_service", metadata_metrics_endpoint)
        ice_metadata_source_service_container = ServiceContainer(ice_metadata_source_monitor_adapter, messager)

        # set up the visibility datagram source service
        visibility_datagram_source_service = VisibilityDatagramSourceService("localhost", 3000, datagram_endpoint, datagram_control_endpoint)
        visibility_datagram_source_monitor_adapter = MonitorAdapter(visibility_datagram_source_service, "visibility_datagram_source_service", datagram_metrics_endpoint)
        visibility_datagram_source_service_container = ServiceContainer(visibility_datagram_source_monitor_adapter, messager)

        # set up the align service
        align_service = AlignService(metadata_endpoint, datagram_endpoint, chunk_endpoint, align_control_endpoint)
        align_monitor_adapter = MonitorAdapter(align_service, "align_service", align_metrics_endpoint)
        align_service_container = ServiceContainer(align_monitor_adapter, messager)
        
        to_string_service = ToStringService(chunk_endpoint, chunk_string_endpoint, to_string_control_endpoint)
        to_string_monitor_adapter = MonitorAdapter(to_string_service, "to_string_service", to_string_metrics_endpoint)
        to_string_service_container = ServiceContainer(to_string_monitor_adapter, messager)

        string_writer_service = StringWriterService(chunk_string_endpoint, string_writer_control_endpoint, "output.txt")
        string_writer_monitor_adapter = MonitorAdapter(string_writer_service, "string_writer_service", string_writer_metrics_endpoint)
        string_writer_service_container = ServiceContainer(string_writer_monitor_adapter, messager)

        monitor_service = GenericMonitorService([datagram_metrics_endpoint, metadata_metrics_endpoint, align_metrics_endpoint, to_string_metrics_endpoint, string_writer_metrics_endpoint])
        monitor_service_container = ServiceContainer(monitor_service, messager)

    
        logging.info("Starting services")
        monitor_service_container.start()
        visibility_datagram_source_service_container.start()
        ice_metadata_source_service_container.start()
        align_service_container.start()
        to_string_service_container.start()
        string_writer_service_container.start()
    
        logging.info("Starting processing")
        messager.publish(datagram_control_endpoint, StringMessage("Start"))
        messager.publish(metadata_control_endpoint, StringMessage("Start"))
        messager.publish(align_control_endpoint, StringMessage("Start"))
        messager.publish(to_string_control_endpoint, StringMessage("Start"))
        messager.publish(string_writer_control_endpoint, StringMessage("Start"))

        # start playback
        logging.info("Starting playback")
        playback = Playback("testbed_data")
        playback.playback("data/ade1card.ms")
        playback.wait()
    
        time.sleep(10)
        messager.publish(datagram_control_endpoint, StringMessage("Stop"))

        time.sleep(5)
        messager.publish(metadata_control_endpoint, StringMessage("Stop"))

        time.sleep(5)
        messager.publish(align_control_endpoint, StringMessage("Stop"))

        time.sleep(5)
        messager.publish(to_string_control_endpoint, StringMessage("Stop"))

        time.sleep(5)
        messager.publish(string_writer_control_endpoint, StringMessage("Stop"))
    
        ice_metadata_source_service_container.terminate()
        visibility_datagram_source_service_container.terminate()
        align_service_container.terminate()
        to_string_service_container.terminate()
        string_writer_service_container.terminate()
        monitor_service_container.terminate()
    
        # stop Ice
        logging.info("stopping Ice")
        ice_runner.stop()
    
    
    
if __name__ == '__main__':
    unittest.main()
    
