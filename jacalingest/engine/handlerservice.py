import itertools
import logging

from jacalingest.engine.service import Service

class HandlerService(Service):

    ALWAYS = 1
    PROCESSING = 2

    def __init__(self):
        logging.info("Initializing")

        super(HandlerService, self).__init__()

        self.always_handlers = dict()
        self.always_scheduler = None

        self.processing_handlers = dict()
        self.processing_scheduler = None

        self.nothing_handler = None


    def set_handler(self, endpoint, handler, when):
        if when==HandlerService.ALWAYS:
            self.always_handlers[endpoint] = handler
            self.always_scheduler = itertools.cycle(self.always_handlers)
        elif when==HandlerService.PROCESSING:
            self.processing_handlers[endpoint] = handler
            self.processing_scheduler = itertools.cycle(self.processing_handlers)

    def set_nothing_processed_handler(self, handler):
        self.nothing_handler = handler

    def always_step(self):
        processed = False
        while True:
            for i in range(len(self.always_handlers)):
                endpoint = self.always_scheduler.next()
                handler = self.always_handlers[endpoint]
                message = endpoint.poll()
                if message:
                    handler(message)
                    processed = True
                    break
            else:
                break;
        return processed

    def processing_step(self):
        for i in range(len(self.processing_handlers)):
            endpoint = self.processing_scheduler.next()
            handler = self.processing_handlers[endpoint]
            message = endpoint.poll()
            if message:
                handler(message)
                break
        else:
            if self.nothing_handler:
                return self.nothing_handler()
        return True

