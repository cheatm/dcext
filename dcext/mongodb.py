from dcext.framework.transmit import Publisher
from pymongo.collection import Collection
import logging


class MongoDBPublisher(Publisher):

    def __init__(self, target, logger):
        assert isinstance(target, Collection)
        assert isinstance(logger, Collection)
        self.target = target
        self.logger = logger
    
    def pub(self, content):
        try:
            self.target.insert_one(content)
        except Exception as e:
            logging.error("insert target | %s | %s", e, content)
    
    def log(self, content):
        try:
            self.logger.insert_one(content)
        except Exception as e:
            logging.error("insert logger | %s | %s", e, content)
