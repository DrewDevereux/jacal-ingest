import logging

from jacalingest.engine.service import Service
from jacalingest.engine.abstractcontainer import AbstractContainer
from jacalingest.monitoringandcontrol import Metrics

class MonitorAdapter(Service, AbstractContainer):
    def __init__(self, service, label, metrics_endpoint):
        self.service = service
        service.set_messager(self)

        self.metrics_endpoint = metrics_endpoint
        self.metrics = Metrics(label)

    def start(self):
        self.service.start()

    def terminate(self):
        self.service.terminate()
        logging.info("Publishing metrics on termination")
        self.messager.publish(self.metrics_endpoint, self.metrics)

    def tick(self):
        state = self.service.tick()
        if state is not None:
            if state != self.metrics.current_state:
                self.metrics.state(state)
                logging.info("Publishing metrics on change of state.")
                self.messager.publish(self.metrics_endpoint, self.metrics)
        return state

    def publish(self, endpoint, message):
        self.messager.publish(endpoint, message)
        self.metrics.sent(endpoint.topic)

    def poll(self, endpoint):
        message = self.messager.poll(endpoint)

        if message is None:
            self.metrics.polled(endpoint.topic)
        else:
            self.metrics.received(endpoint.topic)

        return message
