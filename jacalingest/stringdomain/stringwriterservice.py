import logging

from jacalingest.engine.handlerservice import HandlerService

class StringWriterService(HandlerService):

    IDLE_STATE = 1
    PROCESSING_STATE = 2

    def __init__(self, string_endpoint, control_endpoint, output_path):
        logging.info("Initializing")

        super(StringWriterService, self).__init__(self.IDLE_STATE)

        self.set_handler(string_endpoint, self.handle_string, [self.PROCESSING_STATE])
        self.set_handler(control_endpoint, self.handle_control, [self.IDLE_STATE, self.PROCESSING_STATE])
 
        self.output_path = output_path

    def handle_control(self, message):
        if message.payload == "Start":
            logging.info("Received 'Start' control message.")
            return self.PROCESSING_STATE
        elif message.payload == "Stop":
            logging.info("Received 'Stop' control message.")
            return self.IDLE_STATE
        else:
            logging.info("Received unknown control message '{}'".format(message.payload))
            return None

    def handle_string(self, message):
        logging.debug("Received message '%s'" % message.payload)
        with open(self.output_path, "a") as f:
            f.write("%s\n" % message.payload)
        return None
