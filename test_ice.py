import logging

from jacalingest.engine.ascii.asciifilemessagingsystem import AsciiFileMessagingSystem
from jacalingest.ingest.icemetadatasourceservice import IceMetadataSourceService

def main():
    logging.basicConfig(level=logging.INFO, format='%(threadName)s, %(module)s: %(message)s')

    # set up messaging system. This is all the rest of the system will see
    messaging_system = AsciiFileMessagingSystem()

    service = IceMetadataSourceService(messaging_system, "localhost", "4061", "IceStorm/TopicManager@IceStorm.TopicManager", "metadata", "IngestPipeline", "playbackstatus")


if __name__ == "__main__":
    main()

