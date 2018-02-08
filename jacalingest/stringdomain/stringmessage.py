import logging

from jacalingest.engine.messaging.message import Message

class StringMessage(Message):
    def __init__(self, payload):
        logging.debug("initializing")

        self.payload = payload

    @staticmethod
    def serialize(self):
        return self.payload

    @staticmethod
    def deserialize(serialized):
        return StringMessage(serialized)
