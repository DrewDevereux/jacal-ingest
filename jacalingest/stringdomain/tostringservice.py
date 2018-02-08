import logging

from jacalingest.engine.handlerservice import HandlerService
from jacalingest.stringdomain.stringmessage import StringMessage

class ToStringService(HandlerService):
    IDLE_STATE = 1
    PROCESSING_STATE = 2

    def __init__(self, in_endpoint, out_endpoint, control_endpoint):
        logging.debug("Initializing")

        super(ToStringService, self).__init__(self.IDLE_STATE)

        self.set_handler(in_endpoint, self.handle_message, [self.PROCESSING_STATE])
        self.set_handler(control_endpoint, self.handle_control, [self.IDLE_STATE, self.PROCESSING_STATE])

        self.out_endpoint = out_endpoint

    def handle_control(self,message):
        if message.payload == "Start":
            logging.info("Received 'Start' control message")
            return self.PROCESSING_STATE
        elif message.payload == "Stop":
            logging.info("Received 'Stop' control message")
            return self.IDLE_STATE
        else:
            logging.info("Received unknown control message '{}'".format(message.payload))
            return None

    def handle_message(self,message):
        self.out_endpoint.publish(StringMessage(str(message)))
        return None

