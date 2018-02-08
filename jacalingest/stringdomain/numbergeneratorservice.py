import logging

from jacalingest.engine.service import Service
from jacalingest.stringdomain.stringmessage import StringMessage

class NumberGeneratorService(Service):

    def __init__(self, endpoint):
        logging.info("initializing")

        super(NumberGeneratorService, self).__init__()

        self.endpoint = endpoint
        self.numbers = self.generate_numbers()

    def generate_numbers(self):
        i=0
        while True:
            i+=1
            yield i

    def start(self):
        logging.info("Starting")

    def tick(self):
        number = self.numbers.next()

        logging.debug("Publishing message %s" % str(number))
        self.messager.publish(self.endpoint, StringMessage(str(number)))
        return None

    def terminate(self):
        logging.info("Terminating")

