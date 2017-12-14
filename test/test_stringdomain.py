import logging
import time
import unittest

from jacalingest.engine.queuemessagingsystem import QueueMessagingSystem
from jacalingest.engine.service import Service

from jacalingest.stringdomain.lettergeneratorservice import LetterGeneratorService
from jacalingest.stringdomain.stringconcatenatorservice import StringConcatenatorService
from jacalingest.stringdomain.tostringservice import ToStringService
from jacalingest.stringdomain.stringwriterservice import StringWriterService

class TestStringDomain(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    def test(self):
        queue_messaging_system = QueueMessagingSystem()
        messaging_context = {"DATA": (queue_messaging_system, Service.WHEN_PROCESSING),
                             "M&C": (queue_messaging_system, Service.ALWAYS)}

        uppercase_letter_generator_service = LetterGeneratorService(messaging_context, "upper", "DATA", upper=True)
        lowercase_letter_generator_service = LetterGeneratorService(messaging_context, "lower", "DATA")
        string_concatenator_service = StringConcatenatorService(messaging_context, "upper", "lower", "concatenation", "DATA")
        string_writer_service = StringWriterService(messaging_context, "concatenation", "DATA", None, None, "output.txt")


        # start them, wait ten seconds, stop them
        logging.debug("starting services")
        string_writer_service.start()
        string_concatenator_service.start()
        uppercase_letter_generator_service.start()
        lowercase_letter_generator_service.start()

        time.sleep(1)

        logging.debug("starting processing")
        string_writer_service.start_processing()
        string_concatenator_service.start_processing()
        uppercase_letter_generator_service.start_processing()
        lowercase_letter_generator_service.start_processing()

        logging.debug("sleeping")
        time.sleep(10)

        logging.debug("stopping processing")
        uppercase_letter_generator_service.stop_processing()
        lowercase_letter_generator_service.stop_processing()
        string_concatenator_service.stop_processing()
        string_writer_service.stop_processing()

        time.sleep(1)

        logging.debug("stopping services")
        uppercase_letter_generator_service.terminate()
        lowercase_letter_generator_service.terminate()
        string_concatenator_service.terminate()
        string_writer_service.terminate()


if __name__ == '__main__':
    unittest.main()

