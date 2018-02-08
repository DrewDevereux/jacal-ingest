import logging
from string import ascii_lowercase
from string import ascii_uppercase

from jacalingest.engine.service import Service
from jacalingest.stringdomain.stringmessage import StringMessage

class LetterGeneratorService(Service):
    """A service that cycles through the alphabet, generating single letter messages"""

    def __init__(self, endpoint, upper=False):
        """Initialise an instance.

        Arguments:
        messaging_system -- the messaging system to communicate on
        topic -- the name of the messaging system topic to generate messages for
        upper -- a boolean indicating whether to generate upper case letters (default lower case)
        """
        logging.info("initializing")

        super(LetterGeneratorService, self).__init__()
        self.endpoint = endpoint
        self.messager = None

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

    def start(self):
        logging.info("Starting")

    def tick(self):
        """Performs a single iteration of processing; that is, publishes a single message containing the next letter in the cycle
        """
        letter = self.letters.next()

        logging.debug("Publishing message %s" % str(letter))
        self.messager.publish(self.endpoint, StringMessage(letter))
        return None

    def terminate(self):
        logging.info("Terminating")

