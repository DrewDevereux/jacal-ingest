import logging

from jacalingest.engine.drainservice import DrainService

class StringWriterService(DrainService):
    def __init__(self, messaging_context, string_topic, string_topic_group, status_topic, status_topic_group, output_path, terminate=False):
        logging.debug("Initializing")

        topic_map = {string_topic: string_topic_group}
        handlers = {string_topic: self.handle_string}

        if status_topic:
            topic_map[status_topic] = status_topic_group
            handlers[status_topic] = self.handle_status

        super(StringWriterService, self).__init__(messaging_context, topic_map, handlers, terminate=terminate)
        self.output_path = output_path

    def handle_status(self, message):
        if message == "Finished":
            logging.info("Received 'Finished' message.")
            self.drain()

    def handle_string(self, message):
        logging.debug("Received message '%s'" % message)
        with open(self.output_path, "a") as f:
            f.write("%s\n" % message)
