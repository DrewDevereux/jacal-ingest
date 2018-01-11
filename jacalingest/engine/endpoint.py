import logging

class Endpoint(object):
    def __init__(self, stream):
        logging.info("Initializing")

        self.stream = stream
        self.cursor = self.stream.subscribe()

    def publish(self, message):
        self.stream.publish(message)

    def poll(self):
        (message, cursor) = self.stream.poll(self.cursor)
        self.cursor = cursor
        return message

