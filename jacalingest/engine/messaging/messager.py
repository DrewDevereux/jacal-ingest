import logging

from jacalingest.engine.abstractcontainer import AbstractContainer

class Messager(AbstractContainer):
    def __init__(self):
        pass

    def get_endpoint(self, messaging_system, topic, message_class):
        return Messager.Endpoint(messaging_system, topic, message_class)

    def publish(self, endpoint, message):
        endpoint.publish(message)

    def poll(self, endpoint):
        return endpoint.poll()

    class Endpoint(object):
        def __init__(self, messaging_system, topic, messageclass):
            logging.info("Initializing")
    
            self.messaging_system = messaging_system
            self.topic = topic
            self.messageclass = messageclass

            self.cursor = self.messaging_system.subscribe(topic)

        def publish(self, message_object):
            serialized_message = self.messageclass.serialize(message_object)
            self.messaging_system.publish(self.topic, serialized_message)

        def poll(self):
            (serialized_message, cursor) = self.messaging_system.poll(self.topic, self.cursor)
            self.cursor = cursor

            if serialized_message is None:
                return None
            else:
                return self.messageclass.deserialize(serialized_message)

