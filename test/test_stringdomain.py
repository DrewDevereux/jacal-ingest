import logging
import time
import unittest

from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.messager import Messager
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.monitoringandcontrol.genericmonitorservice import GenericMonitorService
from jacalingest.monitoringandcontrol.metrics import Metrics
from jacalingest.monitoringandcontrol.monitoradapter import MonitorAdapter

from jacalingest.stringdomain.lettergeneratorservice import LetterGeneratorService
from jacalingest.stringdomain.stringconcatenatorservice import StringConcatenatorService
from jacalingest.stringdomain.stringmessage import StringMessage
from jacalingest.stringdomain.stringwriterservice import StringWriterService


class TestStringDomain(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        messaging_system = QueueMessagingSystem()
        messager = Messager()

        upper_endpoint = messager.get_endpoint(messaging_system, "upper", StringMessage)
        upper_metrics_endpoint = messager.get_endpoint(messaging_system, "upper_metrics", Metrics)

        lower_endpoint = messager.get_endpoint(messaging_system, "lower", StringMessage)
        lower_metrics_endpoint = messager.get_endpoint(messaging_system, "lower_metrics", Metrics)

        concatenation_endpoint = messager.get_endpoint(messaging_system, "concatenation", StringMessage)
        concatenator_control_endpoint = messager.get_endpoint(messaging_system, "concatenator_control", StringMessage)
        concatenator_metrics_endpoint = messager.get_endpoint(messaging_system, "concatenator_metrics", Metrics)

        writer_control_endpoint = messager.get_endpoint(messaging_system, "writer_control", StringMessage)
        writer_metrics_endpoint = messager.get_endpoint(messaging_system, "writer_metrics", Metrics)

        uppercase_letter_generator_service = LetterGeneratorService(upper_endpoint, upper=True)
        uppercase_letter_generator_monitor_adapter = MonitorAdapter(uppercase_letter_generator_service, "uppercase_letter_generator_service", upper_metrics_endpoint)
        uppercase_letter_generator_service_container = ServiceContainer(uppercase_letter_generator_monitor_adapter, messager)

        lowercase_letter_generator_service = LetterGeneratorService(lower_endpoint)
        lowercase_letter_generator_monitor_adapter = MonitorAdapter(lowercase_letter_generator_service, "lowercase_letter_generator_service", lower_metrics_endpoint)
        lowercase_letter_generator_service_container = ServiceContainer(lowercase_letter_generator_monitor_adapter, messager)

        string_concatenator_service = StringConcatenatorService(upper_endpoint, lower_endpoint, concatenation_endpoint, concatenator_control_endpoint)
        string_concatenator_monitor_adapter = MonitorAdapter(string_concatenator_service, "string_concatenator_service", concatenator_metrics_endpoint)
        string_concatenator_service_container = ServiceContainer(string_concatenator_monitor_adapter, messager)

        string_writer_service = StringWriterService(concatenation_endpoint, writer_control_endpoint, "output.txt")
        string_writer_monitor_adapter = MonitorAdapter(string_writer_service, "string_writer_service", writer_metrics_endpoint)
        string_writer_service_container = ServiceContainer(string_writer_monitor_adapter, messager)

        monitor_service = GenericMonitorService([upper_metrics_endpoint, lower_metrics_endpoint, concatenator_metrics_endpoint, writer_metrics_endpoint])
        monitor_service_container = ServiceContainer(monitor_service, messager)

        # start them, wait ten seconds, stop them
        logging.info("starting services")
        monitor_service_container.start()
        string_writer_service_container.start()
        string_concatenator_service_container.start()
        uppercase_letter_generator_service_container.start()
        lowercase_letter_generator_service_container.start()

        time.sleep(1)

        logging.info("starting processing")
        messager.publish(writer_control_endpoint, StringMessage("Start"))
        messager.publish(concatenator_control_endpoint, StringMessage("Start"))

        logging.info("sleeping")
        time.sleep(10)

        logging.info("stopping processing")
        messager.publish(writer_control_endpoint, StringMessage("Stop"))
        messager.publish(concatenator_control_endpoint, StringMessage("Stop"))

        time.sleep(1)

        logging.info("stopping services")
        uppercase_letter_generator_service_container.terminate()
        lowercase_letter_generator_service_container.terminate()
        string_concatenator_service_container.terminate()
        string_writer_service_container.terminate()
        monitor_service_container.terminate()

if __name__ == '__main__':
    unittest.main()

