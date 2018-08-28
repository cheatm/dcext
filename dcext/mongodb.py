from dcext.framework.transmit import Publisher
from pymongo.collection import Collection
from pymongo.database import Database
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


class MongoDBCappedCointainer(object):

    def __init__(self, db, names, indexes=None, **options):
        assert isinstance(db, Database)
        self.db = db
        self.names = names
        self.options = options
        self.indexes = indexes if isinstance(indexes, list) else []
        cols = self.db.collection_names()
        for name in names:
            if name not in cols:
                self.create(name)

    def create(self, name):
        col = self.db.create_collection(
            name, capped=True, **self.options
        )
        for index in self.indexes:
            col.create_index(index, unique=True, background=True)
    
    def __getitem__(self, key):
        return self.db[key]
    
