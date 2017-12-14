import logging
import Queue

from jacalingest.engine.drainservice import DrainService
from jacalingest.ingest.tosmetadata import TOSMetadata
from jacalingest.ingest.visibilitydatagram import VisibilityDatagram
from jacalingest.ingest.visibilitychunk import VisibilityChunk

class AlignService(DrainService):
    def __init__(self, messaging_context, tos_metadata_topic, tos_metadata_topic_group, visibility_datagram_topic, visibility_datagram_topic_group, visibility_chunk_topic, visibility_chunk_topic_group, tos_metadata_status_topic, visibility_datagram_status_topic, my_status_topic, status_topic_group, terminate=False):
        logging.debug("Initializing")

        topic_map = {tos_metadata_topic:tos_metadata_topic_group,
                     visibility_datagram_topic:visibility_datagram_topic_group,
                     visibility_chunk_topic:visibility_chunk_topic_group,
                     tos_metadata_status_topic:status_topic_group,
                     visibility_datagram_status_topic:status_topic_group,
                     my_status_topic:status_topic_group}
        handlers = {tos_metadata_topic:self.handle_tos_metadata,
                    visibility_datagram_topic:self.handle_visibility_datagram,
                    tos_metadata_status_topic:self.handle_tos_metadata_status,
                    visibility_datagram_status_topic:self.handle_visibility_datagram_status}
        super(AlignService, self).__init__(messaging_context, topic_map, handlers, terminate=terminate)

        self.visibility_chunk_topic = visibility_chunk_topic
        self.my_status_topic = my_status_topic

        self.tos_draining = False
        self.vis_draining = False

        self.metadata_queue = Queue.Queue()
        self.datagram_queue = Queue.Queue()

        self.current_chunk = None
        self.current_visibility = None

    def handle_tos_metadata(self, message):
        self.metadata_queue.put(TOSMetadata.deserialize(message))

    def handle_visibility_datagram(self, message):
        self.datagram_queue.put(VisibilityDatagram.deserialize(message))

    def handle_tos_metadata_status(self, message):
        if message == "Finished":
            logging.info("Received 'Finished' message (TOS metadata)")
            self.tos_draining = True
            if self.vis_draining:
                self.drain()

    def handle_visibility_datagram_status(self, message):
        if message == "Finished":
            logging.info("Received 'Finished' message (visibility datagrams)")
            self.vis_draining = True
            if self.tos_draining:
                self.drain()

    def step(self):
        if not self.current_chunk:
            if not self.metadata_queue.empty():
               tos_metadata = self.metadata_queue.get_nowait()
               logging.info("Received TOS metadata with timestamp {}".format(tos_metadata.timestamp))
               self.current_chunk = VisibilityChunk(tos_metadata.timestamp, tos_metadata.scanid, tos_metadata.flagged, tos_metadata.sky_frequency, tos_metadata.target_name, tos_metadata.target_ra, tos_metadata.target_dec, tos_metadata.phase_ra, tos_metadata.phase_dec, tos_metadata.corrmode, tos_metadata.antennas)

        if not self.current_visibility:
            if not self.datagram_queue.empty():
                self.current_visibility = self.datagram_queue.get_nowait()
                logging.info("Received visibility with timestamp {}".format(self.current_visibility.timestamp))

        if not self.current_chunk and not self.current_visibility:
            logging.debug("No message on either stream; returning False.")
            return False

        if self.current_visibility and self.tos_draining and not self.current_chunk:
            logging.info("Discarding visibility because TOS metadata stream has drained.")
            self.current_visibility = None

        if self.current_chunk and self.vis_draining and not self.current_visibility:
            logging.info("Sending chunk because visibilility stream has drained.")
            self.publish(self.visibility_chunk_topic, self.current_chunk.serialize())
            self.current_chunk = None
            
        if self.current_chunk and self.current_visibility:
            if self.current_chunk.timestamp < self.current_visibility.timestamp:
                logging.info("Newer visibility triggers sending of chunk ({} > {})".format(self.current_visibility.timestamp, self.current_chunk.timestamp))
                self.publish(self.visibility_chunk_topic, self.current_chunk.serialize())
                self.current_chunk = None
            else:
                if self.current_chunk.timestamp == self.current_visibility.timestamp:
                    logging.info("Adding a visibility to the current chunk ({} = {})".format(self.current_visibility.timestamp, self.current_chunk.timestamp))
                    self.current_chunk.add_visibility(self.current_visibility)
                else:
                    logging.info("Discarding an old visibility ({} < {})".format(self.current_visibility.timestamp, self.current_chunk.timestamp))
                self.current_visibility = None

        return True

    def drained(self):
        logging.info("Drained; sending 'Finished' message on topic {}".format(self.my_status_topic))
        self.publish(self.my_status_topic, "Finished")
        super(AlignService, self).drained()


