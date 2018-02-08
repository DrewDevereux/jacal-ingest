import logging

class Message:
    def __init__(self, payload):
        logging.debug("initializing")

        self.payload = payload

    @staticmethod
    def serialize(self):
        raise NotImplementedError

    @staticmethod
    def deserialize(serialized):
        raise NotImplementedError
