import logging

class Stream(object):

    def __init__(self, messaging_system, topic):
        logging.info("Initializing")

        self.messaging_system = messaging_system
        self.topic = topic

    def publish(self, message):
        self.messaging_system.publish(self.topic, message)

    def subscribe(self):
        return self.messaging_system.subscribe(self.topic)

    def poll(self, cursor):
        return self.messaging_system.poll(self.topic, cursor)

