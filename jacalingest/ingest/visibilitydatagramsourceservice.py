import logging
import Queue
import socket
import struct
import threading
import time

from jacalingest.ingest.visibilitydatagram import VisibilityDatagram
from jacalingest.engine.statefulservice import StatefulService

# ISSUES: Need to support max_beamid and max_slice

class VisibilityDatagramSourceService(StatefulService):
    IDLE_STATE = 1
    PROCESSING_STATE = 2

    def __init__(self, host, port, visibility_datagram_endpoint, control_endpoint, **kwargs):
        logging.debug("Initializing")

        super(VisibilityDatagramSourceService, self).__init__(self.IDLE_STATE)

        self.visibility_datagram_endpoint = visibility_datagram_endpoint
        self.control_endpoint = control_endpoint

        buffersize = kwargs.get('buffersize', 1024 * 1024 * 16)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buffersize)
        self.sock.setblocking(0) # non-blocking
        self.sock.bind((host, port))

        self.buffer = Queue.Queue()

        self.udp_datagrams = dict()

        self.socketthread = threading.Thread(target=self.readFromSocket, name="%s socket thread" % self.__class__.__name__)
        self.__stopthread__ = False

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

    def stateful_tick(self, state):
        logging.debug("in stateful_tick with state {}".format(state))
        always_state = self.always_tick()

        if always_state is not None:
            logging.info("New state is {}".format(always_state))
            state = always_state

        if state == self.PROCESSING_STATE:
            processing_state = self.processing_tick()
            if processing_state is None:
                return always_state
            else:
                logging.info("New state is {}".format(processing_state))
                return processing_state

    def always_tick(self):
        logging.debug("In always tick")
        message = self.messager.poll(self.control_endpoint)
        while message is not None:
            if message.payload == "Start":
                logging.info("Received 'Start' control message")
                self.socketthread.start()
                return self.PROCESSING_STATE
            elif message.payload == "Stop":
                logging.info("Received 'Stop' control message")
                self.__stopthread__ = True
                return self.IDLE_STATE
            else:
                logging.info("Received unknown control message: {}".format(message.payload))
                return None
            message = self.messager.poll(self.control_endpoint)

    def processing_tick(self):
        logging.debug("In processing_tick")
        while True:
            try:
                data = self.buffer.get(block=False)
            except Queue.Empty:
                logging.debug("Queue is empty")
                return None
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
                self.messager.publish(self.visibility_datagram_endpoint, message)
                logging.debug("Published a data message.")

                del self.udp_datagrams[(udp_datagram.timestamp, udp_datagram.channel)]
                return None


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

