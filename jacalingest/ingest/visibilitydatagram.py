import logging
import struct

from jacalingest.engine.messaging.message import Message

class VisibilityDatagram(Message):
    def __init__(self, timestamp, block, card, channel, freq, beamid, visibilities):
        self.timestamp = timestamp
        self.block = block
        self.card = card
        self.channel = channel
        self.freq = freq
        self.beamid = beamid
        self.visibilities = visibilities

    headerformat = "!QIIIdI"
    headersize = struct.calcsize(headerformat)
    visibilityformat = "ff"
    visibilitysize = struct.calcsize(visibilityformat)

    @staticmethod
    def serialize(deserialized):
        return struct.pack(VisibilityDatagram.headerformat + VisibilityDatagram.visibilityformat*len(deserialized.visibilities), deserialized.timestamp, deserialized.block, deserialized.card, deserialized.channel, deserialized.freq, deserialized.beamid, *(e for v in deserialized.visibilities for e in v))

    @staticmethod
    def deserialize(serialized):
        number_of_visibilities = (len(serialized) - VisibilityDatagram.headersize) / VisibilityDatagram.visibilitysize

        (timestamp, block, card, channel, freq, beamid) = struct.unpack(VisibilityDatagram.headerformat, serialized[:VisibilityDatagram.headersize])

        vit = iter(struct.unpack(VisibilityDatagram.visibilityformat*number_of_visibilities, serialized[VisibilityDatagram.headersize:]))
        visibilities = zip(vit, vit)

        return VisibilityDatagram(timestamp, block, card, channel, freq, beamid, visibilities)

    def __str__(self):
        return "(%s, %d, %d, %d, %f, %d, (%s))" % (self.timestamp, self.block, self.card, self.channel, self.freq, self.beamid, ",".join("(%f,%f)"%(r,c) for (r,c) in self.visibilities))

