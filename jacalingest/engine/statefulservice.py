import logging
import threading
import time

from jacalingest.engine.service import Service

class StatefulService(Service):

    OFF_STATE = 0

    def __init__(self, starting_state):
        logging.info("Initializing")
        self._name = self.__class__.__name__
        self.state = self.OFF_STATE
        self.starting_state = starting_state

    def start(self):
        logging.info("Starting")
        self.state = self.starting_state

    def tick(self):
        if self.state == self.OFF_STATE:
            return self.OFF_STATE

        state = self.stateful_tick(self.state)
        if state is not None:
            self.state = state

        return self.state

    def stateful_tick(self, state):
        raise NotImplementedError

    # tell the service to terminate and die
    def terminate(self):
        logging.info("Terminating")
        self.state = self.OFF_STATE

