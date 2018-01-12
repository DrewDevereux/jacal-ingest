import logging

from jacalingest.engine.messaging.stream import Stream

class MessageStream(object):

    def __init__(self, messaging_system, topic, messageclass):
        logging.info("Initializing")

        self.stream = Stream(messaging_system, topic)
        self.messageclass = messageclass

    def publish(self, message_object):
        serialized_message = self.messageclass.serialize(message_object)
        self.stream.publish(serialized_message)

    def subscribe(self):
        return self.stream.subscribe()

    def poll(self, cursor):
        (serialized_message, cursor) = self.stream.poll(cursor)
        if not serialized_message:
            return (None, cursor)
        return (self.messageclass.deserialize(serialized_message), cursor)

