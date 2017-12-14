"""This module contains dummy classes for generating messages"""
import logging
from string import ascii_lowercase
from string import ascii_uppercase

from jacalingest.engine.service import Service
from jacalingest.engine.messagingsystem import MessagingSystem

class LetterGeneratorService(Service):
    """A service that cycles through the alphabet, generating single letter messages"""

    def __init__(self, messaging_context, topic, topic_group, upper=False):
        """Initialise an instance.

        Arguments:
        messaging_system -- the messaging system to communicate on
        topic -- the name of the messaging system topic to generate messages for
        upper -- a boolean indicating whether to generate upper case letters (default lower case)
        """
        logging.debug("initializing")

        super(LetterGeneratorService, self).__init__(messaging_context, {topic: topic_group}, None)

        self.topic = topic
        self.letters = self.generate_letters(upper)

    def generate_letters(self, upper):
        """A generator for a neverending cycle through the alphabet

        Arguments:
        upper -- a boolean indicating whether to generator upper case letters, otherwise lower case
        """
        if upper:
            alphabet = ascii_uppercase
        else:
            alphabet = ascii_lowercase

        while True:
            for letter in alphabet:
                yield letter 

    def drain(self):
        super(LetterGeneratorService, self).drain()
        self.drained()

    def step(self):
        """Performs a single iteration of processing; that is, publishes a single message containing the next letter in the cycle
        """
        letter = self.letters.next()

        logging.debug("Publishing message %s" % str(letter))
        self.publish(self.topic, str(letter)) # shortcut; sending string directly rather than putting into a message
        return True

