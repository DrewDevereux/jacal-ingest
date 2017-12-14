import logging

from jacalingest.engine.service import Service
from jacalingest.engine.messagingsystem import MessagingSystem

class NumberGeneratorService(Service):
    def __init__(self, messaging_context, topic, topic_group):
        logging.info("initializing")

        super(NumberGeneratorService, self).__init__(messaging_context, {topic: topic_group}, None)

        self.topic = topic

        self.numbers = self.generate_numbers()

    def generate_numbers(self):
        while True:
            for i in range(26):
                yield i

    def drain(self):
        super(NumberGeneratorService, self).drain()
        self.drained()

    def step(self):
        number = self.numbers.next()

        logging.debug("Publishing message %s" % str(number))
        self.publish(self.topic, str(number))
        return True

