import logging
import struct

from jacalingest.engine.message import Message
from jacalingest.ingest.tosmetadata import TOSMetadata

class VisibilityChunk(Message):
    J2000 = 1
    AZEL = 2

    def __init__(self, timestamp, scanid, flagged, sky_frequency, target_name, target_ra, target_dec, phase_ra, phase_dec, corrmode, antennas, visibility_count=0):
        self.timestamp = timestamp
        self.scanid = scanid
        self.flagged = flagged
        self.sky_frequency = sky_frequency
        self.target_name = target_name
        self.target_ra = target_ra
        self.target_dec = target_dec
        self.phase_ra = phase_ra
        self.phase_dec = phase_dec
        self.corrmode = corrmode
        self.antennas = antennas.copy()

        self.visibility_count = visibility_count

    def add_visibility(self, visibility):
        self.visibility_count = self.visibility_count + 1

    def serialize(self):
        serialized = (VisibilityChunk.TupleSerializer("!Qi?d").serialize((self.timestamp, self.scanid, self.flagged, self.sky_frequency))
                     + VisibilityChunk.StringSerializer().serialize(self.target_name)
                     + VisibilityChunk.TupleSerializer("!dddd").serialize((self.target_ra, self.target_dec, self.phase_ra, self.phase_dec))
                     + VisibilityChunk.StringSerializer().serialize(self.corrmode)
                     + VisibilityChunk.DictSerializer(VisibilityChunk.StringSerializer(), VisibilityChunk.TupleSerializer("!ddddd??")).serialize(self.antennas)
                     + VisibilityChunk.TupleSerializer("!I").serialize((self.visibility_count,)))
        return serialized

    @staticmethod
    def deserialize(serialized):
        ((timestamp, scanid, flagged, sky_frequency), tail) = VisibilityChunk.TupleSerializer("!Qi?d").deserialize_next(serialized)
        (target_name, tail) = VisibilityChunk.StringSerializer().deserialize_next(tail)
        ((target_ra, target_dec, phase_ra, phase_dec), tail) = VisibilityChunk.TupleSerializer("!dddd").deserialize_next(tail)
        (corrmode, tail) = VisibilityChunk.StringSerializer().deserialize_next(tail)
        (antennas, tail) = VisibilityChunk.DictSerializer(VisibilityChunk.StringSerializer(), VisibilityChunk.TupleSerializer("!ddddd??")).deserialize_next(tail)
        ((visibility_count,), tail) = VisibilityChunk.TupleSerializer("!I").deserialize_next(tail)

        return VisibilityChunk(timestamp, scanid, flagged, sky_frequency, target_name, target_ra, target_dec, phase_ra, phase_dec, corrmode, antennas, visibility_count)

    def __str__(self):

        return "VisibilityChunk:\n\tTimestamp: {}\n\tScan ID: {}\n\tFlagged: {}\n\tSky frequency: {}\n\tTarget name: {}\n\tTarget direction: ({}, {})\n\tPhase direction: ({}, {})\n\tCorr mode: {}\n\tAntennas: [{}]\n\tNumber of visibilities: {}".format(self.timestamp, self.scanid, self.flagged, self.sky_frequency, self.target_name, self.target_ra, self.target_dec, self.phase_ra, self.phase_dec, self.corrmode, str(self.antennas), self.visibility_count)


    class TupleSerializer(object):
        def __init__(self, format):
            self._format = format
            self._size = struct.calcsize(self._format)

        def serialize(self, args):
            return struct.pack(self._format, *args)

        def deserialize_next(self, serialized):
            head = struct.unpack(self._format, serialized[:self._size])
            tail = serialized[self._size:]
            return (head, tail)

    class StringSerializer(object):
        def __init__(self):
            pass

        _lenformat = "I"
        _lensize = struct.calcsize(_lenformat)

        def serialize(self, the_string):
            return struct.pack(VisibilityChunk.StringSerializer._lenformat, len(the_string)) + the_string

        def deserialize_next(self, serialized):
            (string_len,) = struct.unpack(VisibilityChunk.StringSerializer._lenformat, serialized[:VisibilityChunk.StringSerializer._lensize])
            head = serialized[VisibilityChunk.StringSerializer._lensize:(VisibilityChunk.StringSerializer._lensize+string_len)]
            tail = serialized[(VisibilityChunk.StringSerializer._lensize+string_len):]
            return (head, tail)

    class ListSerializer(object):
        def __init__(self, contentserializer):
            self._serializer = contentserializer

        _lenformat = "I"
        _lensize = struct.calcsize(_lenformat)


        def serialize(self, the_list):
            serialized = struct.pack(VisibilityChunk.ListSerializer._lenformat, len(the_list))
            for e in the_list:
                serialized = serialized + self._serializer.serialize(e)

            return serialized

        def deserialize_next(self, serialized):
            the_list = list()

            (list_len,) = struct.unpack(VisibilityChunk.ListSerializer._lenformat, serialized[:VisibilityChunk.ListSerializer._lensize])

            tail = serialized[VisibilityChunk.ListSerializer._lensize:]
            for s in range(list_len):
               (head, tail) = self._serializer.deserialize_next(tail)
               the_list.append(head)

            return (the_list, tail)

    class DictSerializer(object):
        def __init__(self, keyserializer, valueserializer):
             self._keyserializer = keyserializer
             self._valueserializer = valueserializer
 
        _lenformat = "I"
        _lensize = struct.calcsize(_lenformat)
 
        def serialize(self, the_dict):
            serialized = struct.pack(VisibilityChunk.DictSerializer._lenformat, len(the_dict))
            for (k,v) in the_dict.iteritems():
                serialized = serialized + self._keyserializer.serialize(k) + self._valueserializer.serialize(v)
            return serialized

        def deserialize_next(self, serialized):
            the_dict = dict()

            (dict_len,) = struct.unpack(VisibilityChunk.DictSerializer._lenformat, serialized[:VisibilityChunk.DictSerializer._lensize])

            tail = serialized[VisibilityChunk.DictSerializer._lensize:]
            for s in range(dict_len):
               (key, tail) = self._keyserializer.deserialize_next(tail)
               (value, tail) = self._valueserializer.deserialize_next(tail)
               the_dict[key] = value

            return (the_dict, tail)

