import logging

class Message:
    def __init__(self, payload):
        logging.debug("initializing")

        self.payload = payload

    def serialize(self):
        raise NotImplementedError

    def deserialize(serialized):
        raise NotImplementedError
