import logging

from jacalingest.engine.service import Service

class GenericMonitorService(Service):
    def __init__(self, endpoints_to_monitor):
        logging.info("initializing")

        super(GenericMonitorService, self).__init__()
        self.endpoints = endpoints_to_monitor

    def start(self):
        logging.info("Starting")

    def tick(self):
        for endpoint in self.endpoints:
            message = self.messager.poll(endpoint)
            if message is not None:
                logging.info(str(message))
        return None

    def terminate(self):
        logging.info("Terminating")

