import logging
import Queue

from jacalingest.engine.handlerservice import HandlerService
from jacalingest.stringdomain.stringmessage import StringMessage

class StringConcatenatorService(HandlerService):
    IDLE_STATE = 1
    PROCESSING_STATE = 2

    def __init__(self, first_endpoint, second_endpoint, concatenation_endpoint, control_endpoint):
        logging.info("Initializing")

        super(StringConcatenatorService, self).__init__(self.IDLE_STATE)

        self.set_handler(first_endpoint, self.handle_first, [self.PROCESSING_STATE])
        self.set_handler(second_endpoint, self.handle_second, [self.PROCESSING_STATE])
        self.set_handler(control_endpoint, self.handle_control, [self.IDLE_STATE, self.PROCESSING_STATE])

        self.concatenation_endpoint = concatenation_endpoint

        self.first_queue = Queue.Queue()
        self.second_queue = Queue.Queue()

    def handle_first(self, message):
        self.first_queue.put(message)
        if not self.second_queue.empty():
            first_string = self.first_queue.get_nowait().payload
            second_string = self.second_queue.get_nowait().payload
            concatenation = "{}{}".format(first_string, second_string)
            self.send_concatenation(concatenation)
        return None

    def handle_second(self, message):
        self.second_queue.put(message)
        if not self.first_queue.empty():
            first_string = self.first_queue.get_nowait().payload
            second_string = self.second_queue.get_nowait().payload
            concatenation = "{}{}".format(first_string, second_string)
            self.send_concatenation(concatenation)
        return None

    def send_concatenation(self, concatenation):
        logging.debug("Publishing concatenation {}".format(concatenation))
        self.messager.publish(self.concatenation_endpoint, StringMessage(concatenation))

    def handle_control(self, message):
        if message.payload == "Start":
            logging.info("Received 'Start' control message")
            return self.PROCESSING_STATE
        elif message.payload == "Stop":
            logging.info("Received 'Stop' control message")
            return self.IDLE_STATE
        else:
            logging.info("Received unknown control message: {}".format(message.payload)) 
            return None

