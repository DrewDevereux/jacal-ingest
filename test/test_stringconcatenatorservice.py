import logging
import time
import unittest

from jacalingest.engine.servicecontainer import ServiceContainer
from jacalingest.engine.messaging.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.messaging.messager import Messager
from jacalingest.monitoringandcontrol.metrics import Metrics
from jacalingest.monitoringandcontrol.monitoradapter import MonitorAdapter
from jacalingest.stringdomain.stringconcatenatorservice import StringConcatenatorService
from jacalingest.stringdomain.stringmessage import StringMessage

class TestStringConcatenatorService(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        messaging_system=QueueMessagingSystem()
        messager = Messager()
        metrics_endpoint = messager.get_endpoint(messaging_system, "metrics", Metrics)
        first_endpoint = messager.get_endpoint(messaging_system, "first", StringMessage)
        second_endpoint = messager.get_endpoint(messaging_system, "second", StringMessage)
        concatenation_endpoint = messager.get_endpoint(messaging_system, "concatenation", StringMessage)
        control_endpoint = messager.get_endpoint(messaging_system, "control", StringMessage)

        string_concatenator_service = StringConcatenatorService(first_endpoint, second_endpoint, concatenation_endpoint, control_endpoint)
        monitor_adapter = MonitorAdapter(string_concatenator_service, metrics_endpoint)
        service_container = ServiceContainer(monitor_adapter, messager)

        service_container.start()
        time.sleep(5)
        logging.info("Publishing 'Start' control message")
        messager.publish(control_endpoint, StringMessage("Start"))
        time.sleep(10)
        logging.info("Publishing 'Stop' control message")
        messager.publish(control_endpoint, StringMessage("Stop"))
        time.sleep(5)
        service_container.terminate()
        service_container.wait()

if __name__ == '__main__':
    unittest.main()

