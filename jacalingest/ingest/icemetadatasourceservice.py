import logging
from platform import node
import Queue

import Ice
import IceStorm

import icedefs.askap.interfaces
import icedefs.askap.interfaces.datapublisher

from jacalingest.ingest.tosmetadata import TOSMetadata
from jacalingest.engine.drainservice import DrainService

class IceMetadataSourceService(DrainService):
    def __init__(self, host, port, topic_manager_name, ice_topic_name, adapter_name, messaging_context, metadata_topic, metadata_topic_group, input_status_topic, output_status_topic, status_topic_group, terminate=False):
        logging.info("initializing")

        topic_map = {metadata_topic: metadata_topic_group,
                     input_status_topic: status_topic_group,
                     output_status_topic: status_topic_group}

        handlers = {input_status_topic: self.handle_status}

        super(IceMetadataSourceService, self).__init__(messaging_context, topic_map, handlers, terminate=terminate)

        self.buffer = Queue.Queue()

        self.metadata_topic = metadata_topic
        self.output_status_topic = output_status_topic

        # Set up ICE properties
        ice_properties = Ice.createProperties()
        ice_properties.setProperty("Ice.Default.Locator", "IceGrid/Locator:tcp -h %s -p %s" % (host, port))

        # No tracing
        ice_properties.setProperty("Ice.Trace.Network", "0")
        ice_properties.setProperty("Ice.Trace.Protocol", "0")

        # Increase maximum message size from 1MB to 128MB
        ice_properties.setProperty("Ice.MessageSizeMax", "131072");

        # Disable IPv6. As of Ice 3.5 it is enabled by default
        ice_properties.setProperty("Ice.IPv6", "0");

        # Set the hostname for which clients will initiate a connection
        # to in order to send messages. By default the Ice server will publish
        # all ip addresses and clients will round-robin between them for
        # the puroses of load-balancing. This forces it to only publish
        # a single ip address.
        ice_properties.setProperty("Ice.Default.Host", node());

        ice_properties.setProperty("%s.Endpoints" % adapter_name, "tcp")
        logging.debug("Ice properties are: %s" % str(ice_properties))

        init_data = Ice.InitializationData()
        init_data.properties = ice_properties

        # create the Ice communator
        logging.debug("Creating communicator")
        self.communicator = Ice.initialize(init_data)

        # instantiate a topic manager
        logging.debug("Instantiating topic manager")
        topic_manager = self.communicator.stringToProxy(topic_manager_name)
        topic_manager = IceStorm.TopicManagerPrx.checkedCast(topic_manager)

        # create an adapter
        logging.debug("Creating adapter")
        adapter = self.communicator.createObjectAdapter(adapter_name)

        # create identity
        identity = Ice.Identity()
        uuid = Ice.generateUUID()
        identity.name = uuid

        # create subscriber
        logging.debug("Creating subscriber")
        subscriber = adapter.add(IceMetadataSourceService._IcePublisher(self.buffer), identity)
        subscriber = subscriber.ice_twoway()

        logging.debug("Retrieving topic")
        try:
            topic = topic_manager.retrieve(ice_topic_name)
        except IceStorm.NoSuchTopic:
            try:
                topic = topic_manager.create(ice_topic_name)
            except IceStorm.TopicExists:
                topic = topic_manager.retrieve(ice_topic_name)

        logging.debug("Subscribing")
        qos = {}
        qos["reliability"] = "ordered"

        topic.subscribeAndGetPublisher(qos, subscriber)

        logging.debug("Activating adapter")
        adapter.activate()

    def __del__(self):
        if self.communicator:
            self.communicator.destroy()

    def handle_status(self, message):
        if message == "Finished":
            logging.info("Received 'Finished' message.")
            self.drain()

    def step(self):
        try:
            metadata = self.buffer.get(block=False)
        except Queue.Empty:
            return False
        else:
            #logging.info("metadata.data keys are %s" % sorted(metadata.data.keys()))
            timestamp = metadata.timestamp;
            scanid = metadata.data["scan_id"].value
            flagged = metadata.data["flagged"].value
            sky_frequency = metadata.data["sky_frequency"].value
            target_name = metadata.data["target_name"].value

            target_ra = metadata.data["target_direction"].value.coord1
            target_dec = metadata.data["target_direction"].value.coord1
            assert metadata.data["target_direction"].value.sys == icedefs.askap.interfaces.CoordSys.J2000

            phase_ra = metadata.data["phase_direction"].value.coord1
            phase_dec = metadata.data["phase_direction"].value.coord1
            assert metadata.data["phase_direction"].value.sys == icedefs.askap.interfaces.CoordSys.J2000

            corrmode = metadata.data["corrmode"].value

            antennas = dict()
            for antenna in metadata.data["antennas"].value:
                actual_azel_key = "{}.actual_azel".format(antenna)
                actual_az = metadata.data[actual_azel_key].value.coord1
                actual_el = metadata.data[actual_azel_key].value.coord2
                assert metadata.data[actual_azel_key].value.sys == icedefs.askap.interfaces.CoordSys.AZEL

                actual_radec_key = "{}.actual_radec".format(antenna)
                actual_ra = metadata.data[actual_radec_key].value.coord1
                actual_dec = metadata.data[actual_radec_key].value.coord2
                assert metadata.data[actual_radec_key].value.sys == icedefs.askap.interfaces.CoordSys.J2000

                actual_pol = metadata.data["{}.actual_pol".format(antenna)].value
                flagged = metadata.data["{}.flagged".format(antenna)].value
                on_source = metadata.data["{}.on_source".format(antenna)].value

                antennas[antenna] = (actual_az, actual_el, actual_ra, actual_dec, actual_pol, flagged, on_source)

            message = TOSMetadata(timestamp, scanid, flagged, sky_frequency, target_name, target_ra, target_dec, phase_ra, phase_dec, corrmode, antennas)
            self.publish(self.metadata_topic, message.serialize())
            logging.info("Published a metadata message.")
            return True

    def drained(self):
        if self.output_status_topic:
            self.publish(self.output_status_topic, "Finished")
        super(IceMetadataSourceService, self).drained()


    class _IcePublisher(icedefs.askap.interfaces.datapublisher.ITimeTaggedTypedValueMapPublisher): # need this class because "publish" method of main class shadows "publish" method of Service class

        def __init__(self, buffer):
            self.buffer = buffer

        def publish(self, message, current=None):
            self.buffer.put(message)

