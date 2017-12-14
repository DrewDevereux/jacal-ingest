import logging

from jacalingest.engine.drainservice import DrainService

class ToStringService(DrainService):
    def __init__(self, messaging_context, in_message_class, in_topic, out_topic, topic_group, in_status_topic, out_status_topic, status_group, terminate=False):
        logging.debug("Initializing")

        topic_map={in_topic: topic_group,
                   out_topic: topic_group,
                   in_status_topic: status_group,
                   out_status_topic: status_group}

        handlers={in_topic: self.handle_message,
                   in_status_topic: self.handle_status}

        super(ToStringService, self).__init__(messaging_context, topic_map, handlers, terminate)

        self.in_message_class = in_message_class
        self.out_topic = out_topic
        self.out_status_topic = out_status_topic

    def handle_status(self,message):
        if message == "Finished":
            logging.info("Received 'Finished' message")
            self.drain()

    def handle_message(self,message):
        message_object = self.in_message_class.deserialize(message)
        self.publish(self.out_topic, str(message_object))

    def drained(self):
        if self.out_status_topic:
            logging.info("Drained; Sending 'Finished' message on topic {}".format(self.out_status_topic))
            self.publish(self.out_status_topic, "Finished")

        super(ToStringService, self).drained()

