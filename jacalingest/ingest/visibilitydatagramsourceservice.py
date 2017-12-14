import logging
import Queue
import socket
import struct
import threading
import time

from jacalingest.ingest.visibilitydatagram import VisibilityDatagram
from jacalingest.engine.drainservice import DrainService

# ISSUES: Need to support max_beamid and max_slice

class VisibilityDatagramSourceService(DrainService):
    def __init__(self, host, port, messaging_context, data_topic, data_topic_group, udp_datagram_status_topic, my_status_topic, status_group, terminate=False, **kwargs):
        logging.debug("Initializaing")

        topic_map={data_topic: data_topic_group,
                   udp_datagram_status_topic: status_group,
                   my_status_topic: status_group}

        handlers = {udp_datagram_status_topic: self.handle_status}

        super(VisibilityDatagramSourceService, self).__init__(messaging_context, topic_map, handlers, terminate=terminate)

        buffersize = kwargs.get('buffersize', 1024 * 1024 * 16)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buffersize)
        self.sock.setblocking(0) # non-blocking
        self.sock.bind((host, port))

        self.buffer = Queue.Queue()

        self.data_topic = data_topic
        self.my_status_topic = my_status_topic

        self.udp_datagrams = dict()

        self.socketthread = threading.Thread(target=self.readFromSocket, name="%s socket thread" % self.__class__.__name__)
        self.__stopthread__ = False

    def start_processing(self):
        super(VisibilityDatagramSourceService, self).start_processing()
        self.__stopthread__ = False
        self.socketthread.start()

    def stop_processing(self):
        self.__stopthread__ = True
        super(VisibilityDatagramSourceService, self).stop_processing()

    def drained(self):
        if self.my_status_topic:
            logging.info("Drained; Sending 'Finished' message on topic {}".format(self.my_status_topic))
            self.publish(self.my_status_topic, "Finished")
        super(VisibilityDatagramSourceService, self).drained()

    def readFromSocket(self):
        while not self.__stopthread__:
            try:
                data = self.sock.recv(UDPDatagram.DATAGRAM_SIZE)
            except socket.error as e:
                logging.debug("Socket receive failed with error %s." % str(e))
                time.sleep(0.1)
            else:
                self.buffer.put(data)
                logging.debug("Received some data! Buffer is now approximately %d long" % self.buffer.qsize())

    def handle_status(self, message):
        if message == "Finished":
            logging.info("Received 'Finished' message; draining.")
            self.drain()

    def step(self):
        while True:
            try:
                data = self.buffer.get(block=False)
            except Queue.Empty:
                return False
            else:
                logging.debug("Pulled some data from the buffer! Buffer is now approximately %d long" % self.buffer.qsize())
    
                udp_datagram = UDPDatagram(data)
            
                if (udp_datagram.timestamp, udp_datagram.channel) not in self.udp_datagrams:
                     self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)] = [None, None, None, None]
                self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][udp_datagram.slice] = udp_datagram

                if not self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][0]:
                    continue
                if not self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][1]:
                    continue
                if not self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][2]:
                    continue
                if not self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][3]:
                    continue

                flatvisibilities = (self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][0].visibilities
                                   +self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][1].visibilities
                                   +self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][2].visibilities
                                   +self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)][3].visibilities)
                vit = iter(flatvisibilities)
                visibilities = zip(vit, vit)
                message = VisibilityDatagram(udp_datagram.timestamp, udp_datagram.block, udp_datagram.card, udp_datagram.channel, udp_datagram.freq, udp_datagram.beamid, visibilities)
                self.publish(self.data_topic, message.serialize())
                logging.debug("Published a data message.")

                del self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)]
                return True


class UDPDatagram(object):
    _MAX_BASELINES_PER_SLICE = 657

    _HEADER_FORMAT = "<IIQIIIdIII"
    _HEADER_SIZE = struct.calcsize(_HEADER_FORMAT)

    _VISIBILITIES_FORMAT = "ff"*_MAX_BASELINES_PER_SLICE
    _VISIBILITIES_SIZE = struct.calcsize(_VISIBILITIES_FORMAT)

    DATAGRAM_SIZE = _HEADER_SIZE + _VISIBILITIES_SIZE

    def __init__(self, data):
        self.data = data

        (self.version, self.slice, self.timestamp, self.block, self.card, self.channel, self.freq, self.beamid, self.baseline1, self.baseline2) = struct.unpack(UDPDatagram._HEADER_FORMAT, data[:UDPDatagram._HEADER_SIZE])
        self.visibilities = list(struct.unpack(UDPDatagram._VISIBILITIES_FORMAT, data[UDPDatagram._HEADER_SIZE:]))

        logging.debug("slice %d, timestamp %s, block %d, card %d, channel %d, freq %f, beamid %d, baseline1 %d, baseline2 %d, visibility count %d" % (self.slice, self.timestamp, self.block, self.card, self.channel, self.freq, self.beamid, self.baseline1, self.baseline2, (self.baseline2-self.baseline1+1)))

