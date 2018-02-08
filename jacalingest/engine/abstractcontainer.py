import logging

class AbstractContainer:
    def publish(self, endpoint, message):
        raise NotImplementedError

    def poll(self, endpoint):
        raise NotImplementedError

    def terminate(self):
        raise NotImplementedError
