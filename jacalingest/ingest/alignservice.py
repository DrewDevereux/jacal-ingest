import logging
import Queue

from jacalingest.engine.handlerservice import HandlerService
from jacalingest.ingest.tosmetadata import TOSMetadata
from jacalingest.ingest.visibilitydatagram import VisibilityDatagram
from jacalingest.ingest.visibilitychunk import VisibilityChunk

class AlignService(HandlerService):
    IDLE_STATE = 1
    PROCESSING_STATE = 2

    def __init__(self, metadata_endpoint, datagram_endpoint, chunk_endpoint, control_endpoint):
        logging.info("Initializing")

        super(AlignService, self).__init__(self.IDLE_STATE)

        self.set_handler(metadata_endpoint, self.handle_tos_metadata, [self.PROCESSING_STATE])
        self.set_handler(datagram_endpoint, self.handle_visibility_datagram, [self.PROCESSING_STATE])
        self.set_handler(control_endpoint, self.handle_control, [self.IDLE_STATE, self.PROCESSING_STATE])

        self.chunk_endpoint = chunk_endpoint

        self.tos_draining = False
        self.vis_draining = False

        self.metadata_queue = Queue.Queue()
        self.datagram_queue = Queue.Queue()

        self.current_chunk = None
        self.current_visibility = None


    def handle_control(self, message):
        if message.payload == "Start":
            logging.info("Received 'Start' control message")
            return self.PROCESSING_STATE
        elif message.payload == "Stop":
            logging.info("Received 'Stop' control message")
            return self.IDLE_STATE
        else:
            logging.info("Received unknown control message: {}".format(message.payload))
            return None

    def handle_tos_metadata(self, message):
        self.metadata_queue.put(message)
        self.do_align()
        return None

    def handle_visibility_datagram(self, message):
        self.datagram_queue.put(message)
        self.do_align()
        return None

    def do_align(self):
        if not self.current_chunk:
            if not self.metadata_queue.empty():
               tos_metadata = self.metadata_queue.get_nowait()
               logging.info("Received TOS metadata with timestamp {}".format(tos_metadata.timestamp))
               self.current_chunk = VisibilityChunk(tos_metadata.timestamp, tos_metadata.scanid, tos_metadata.flagged, tos_metadata.sky_frequency, tos_metadata.target_name, tos_metadata.target_ra, tos_metadata.target_dec, tos_metadata.phase_ra, tos_metadata.phase_dec, tos_metadata.corrmode, tos_metadata.antennas)

        if not self.current_visibility:
            if not self.datagram_queue.empty():
                self.current_visibility = self.datagram_queue.get_nowait()
                logging.debug("Received visibility with timestamp {}".format(self.current_visibility.timestamp))

        #if not self.current_chunk and not self.current_visibility:
            #logging.debug("No message on either stream.")

        if self.current_chunk and self.current_visibility:
            if self.current_chunk.timestamp < self.current_visibility.timestamp:
                logging.info("Newer visibility triggers sending of chunk ({} > {})".format(self.current_visibility.timestamp, self.current_chunk.timestamp))
                self.messager.publish(self.chunk_endpoint, self.current_chunk)
                self.current_chunk = None
            else:
                if self.current_chunk.timestamp == self.current_visibility.timestamp:
                    logging.debug("Adding a visibility to the current chunk ({} = {})".format(self.current_visibility.timestamp, self.current_chunk.timestamp))
                    self.current_chunk.add_visibility(self.current_visibility)
                else:
                    logging.info("Discarding an old visibility ({} < {})".format(self.current_visibility.timestamp, self.current_chunk.timestamp))
                self.current_visibility = None

