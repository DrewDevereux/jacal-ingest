import logging
import Queue

from jacalingest.engine.service import Service

class StringConcatenatorService(Service):
    def __init__(self, messaging_context, first_topic, second_topic, concatenation_topic, topic_group):
        logging.debug("Initializing")

        topic_map = {first_topic: topic_group,
                     second_topic: topic_group,
                     concatenation_topic: topic_group}
        handlers = {first_topic: self.handle_first,
                    second_topic: self.handle_second}

        super(StringConcatenatorService, self).__init__(messaging_context, topic_map, handlers)

        self.concatenation_topic = concatenation_topic

        self.first_queue = Queue.Queue()
        self.second_queue = Queue.Queue()

    def handle_first(self, message):
        self.first_queue.put(message)
        if not self.second_queue.empty():
            concatenation = "{}{}".format(self.first_queue.get_nowait(), self.second_queue.get_nowait())
            self.publish(self.concatenation_topic, concatenation)

    def handle_second(self, message):
        self.second_queue.put(message)
        if not self.first_queue.empty():
            concatenation = "{}{}".format(self.first_queue.get_nowait(), self.second_queue.get_nowait())
            self.publish(self.concatenation_topic, concatenation)

