import logging
import threading
import time

from jacalingest.engine.abstractcontainer import AbstractContainer

class ServiceContainer(AbstractContainer):

    def __init__(self, service, messager):
        logging.info("Initializing")

        self._name = self.__class__.__name__
        self.service = service
        service.set_messager(self)

        self.messager = messager

        self.running = False
        self.__thread = threading.Thread(target=self.__run, name="%s main thread" % self._name)

    def start(self):
        logging.info("Starting service...")

        self.service.start()

        if not self.running:
            self.running = True
            self.__thread.start()
        else:
            logging.info("Told to start but was already running.")

    # tell the service to terminate and die
    def terminate(self):
        logging.info("Terminating service...")
        self.service.terminate()

        if self.running:
            self.running = False
        else:
            logging.info("Told to stop but wasn't running.")

    # wait for the service to terminate and die
    def wait(self):
        logging.info("Waiting for thread to die...")
        self.__thread.join()
        logging.info("... wait over")

    def __run(self):
        self.started()

        while self.running:
            if 0 == self.service.tick():
                self.running = False

        self.terminated()

    # called when the service has started
    def started(self):
        logging.info("Started")

    # called when the service has stopped
    def terminated(self):
        logging.info("Terminated")


    # AbstractContainer contract
    def publish(self, endpoint, message):
        self.messager.publish(endpoint, message)

    def poll(self, endpoint):
        return self.messager.poll(endpoint)

