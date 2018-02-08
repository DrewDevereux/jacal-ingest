import logging
import threading
import time

class Service(object):

    def __init__(self):
        logging.info("Initializing")

        self._name = self.__class__.__name__
        self.messager = None

    def set_messager(self, messager):
        self.messager = messager

    def tick(self):
        raise NotImplementedError

