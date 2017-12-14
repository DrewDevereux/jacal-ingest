import logging
import threading
import time

class Service(object):

    OFF = 0
    IDLE = 1
    PROCESSING = 2

    ALWAYS = 1
    WHEN_PROCESSING = 2

    def __init__(self, messaging_context, topic_map, topic_handlers):
        logging.info("Initializing")

        self._name = self.__class__.__name__

        self.messaging_context = messaging_context
        self.topic_map = topic_map
        self.topic_handlers = topic_handlers

        self.cursors = dict()
        for topic in topic_map:
            (messaging_system, _) = messaging_context[topic_map[topic]]
            self.cursors[topic] = messaging_system.subscribe(topic)

        self.state = self.OFF
        self.__thread = threading.Thread(target=self.__run, name="%s main thread" % self._name)

    def start(self):
        if self.state == self.OFF:
            logging.info("Starting")
            self.state = self.IDLE
            self.__thread.start()
        else:
            logging.info("Cannot start unless off.")

    # tell the service to start processing
    def start_processing(self):
        if self.state == self.IDLE:
            logging.info("Starting processing")
            self.state = self.PROCESSING
        else:
            logging.info("Cannot start processing unless idle.")

    # tell the service to stop processing and revert to idle state; can be restarted
    def stop_processing(self):
        if self.state == self.PROCESSING:
            logging.info("Stopping processing")
            self.state = self.IDLE
        else:
            logging.info("Cannot stop processing unless running.")

    # tell the service to terminate and die
    def terminate(self):
        if self.state == self.PROCESSING:
            self.stop_processing()
        if self.state == self.IDLE:
            logging.info("Terminating")
            self.state = self.OFF
        else:
            logging.info("Cannot terminate unless idle.")

    # wait for the service to terminate and die
    def wait(self):
        logging.info("Waiting for thread to die...")
        self.__thread.join()
        logging.info("... wait over")

    # called when the service has stopped
    def terminated(self):
        logging.info("Terminated")

    def __run(self):
        logging.info("Running")

        while self.state != self.OFF:
            for topic in self.topic_map:
                (messaging_system, policy) = self.messaging_context[self.topic_map[topic]]
                if policy == Service.ALWAYS:
                    message = self._poll(messaging_system, topic)
                    while message:
                        if not self.topic_handlers:
                            pass
                        elif topic not in self.topic_handlers:
                            pass
                        elif self.topic_handlers[topic] is None:
                            pass
                        else:
                            self.topic_handlers[topic](message)
                        message = self._poll(messaging_system, topic)
                
            processed_a_message = False
            if self.state == self.PROCESSING:
                for topic in self.topic_map:
                    
                    (messaging_system, policy) = self.messaging_context[self.topic_map[topic]]
                    if policy == Service.WHEN_PROCESSING:
                        message = self._poll(messaging_system, topic)
                        if message:
                            if not self.topic_handlers:
                                pass
                            elif topic not in self.topic_handlers:
                                pass
                            elif self.topic_handlers[topic] is None:
                                pass
                            else:
                                self.topic_handlers[topic](message)
                            processed_a_message = True

            if not self.step() and not processed_a_message:
                self.nothing_to_do()

        self.terminated()

    def step(self):
        return False

    def nothing_to_do(self):
        logging.debug("nothing processed, sleeping...")
        time.sleep(0.1)

    def publish(self, topic, message):
        group = self.topic_map[topic]
        (messaging_system, _) = self.messaging_context[group]
        messaging_system.publish(topic, message)

    def poll(self, topic):
        group = self.topic_map[topic]
        (messaging_system, _) = self.messaging_context[group]
        return self._poll(messaging_system, topic)

    def _poll(self, messaging_system, topic):
        cursor = self.cursors[topic]
        (message, cursor) = messaging_system.poll(topic, cursor)
        self.cursors[topic] = cursor
        return message

