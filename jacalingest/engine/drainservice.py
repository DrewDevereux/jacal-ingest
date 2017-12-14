import logging
import threading
import time

from jacalingest.engine.service import Service

class DrainService(Service):
    def __init__(self, messaging_context, topic_map, topic_handlers, terminate=False):
        self._draining = False
        super(DrainService, self).__init__(messaging_context, topic_map, topic_handlers)
        self.terminate_on_drain = terminate

    def start_processing(self):
        self._draining = False
        super(DrainService, self).start_processing()

    def drain(self):
        logging.info("Draining")
        self._draining = True

    #def is_draining(self):
    #    return self._draining

    def drained(self):
        logging.info("Drained")
        self._draining = False
        self.stop_processing()
        if self.terminate_on_drain:
            self.terminate()

    def nothing_to_do(self):
        if self._draining:
            self.drained()
        else:
            super(DrainService, self).nothing_to_do()
