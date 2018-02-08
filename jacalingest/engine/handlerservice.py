from collections import defaultdict
import itertools
import logging

from jacalingest.engine.statefulservice import StatefulService

class HandlerService(StatefulService):

    def __init__(self, starting_state):
        logging.info("Initializing")

        super(HandlerService, self).__init__(starting_state)

        self.handlers = defaultdict(dict)
        self.cyclers = dict()

    def set_handler(self, endpoint, handler, states):
        for state in states:
            self.handlers[state][endpoint] = handler
            self.cyclers[state] = itertools.cycle(self.handlers[state])

    def stateful_tick(self, state):
        for i in range(len(self.handlers[state])):
            endpoint = self.cyclers[state].next()
            message = self.messager.poll(endpoint)
            if message:
                handler = self.handlers[state][endpoint]
                state = handler(message)
                return state
        return None

