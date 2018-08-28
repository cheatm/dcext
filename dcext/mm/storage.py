from dcext.mongodb import MongoDBCappedCointainer, Collection
import logging
from pymongo.errors import DuplicateKeyError
from pymongo import MongoClient


class BarStorage(object):

    def __init__(self, col):
        assert isinstance(col, Collection)
        self.col = col
        self._last = col.find_one(sort=[("datetime", -1)])
        self.in_db = True
        if not self._last:
            self._last = {"_id": 0, "datetime": "19710101 00:00:00"}
    
    @property
    def last(self):
        if self._last["_id"] > 0:
            doc = self._last.copy()
            doc.pop("_id")
            return doc
        else:
            return None

    def put(self, doc):
        current = self._last["datetime"]
        bar_time = doc["datetime"]
        if bar_time > current:
            self.replace(doc)
            self.insert()
        elif bar_time == current:
            self._last.update(doc)
            self.update(doc)
        else:
            self.update(doc)
    
    def replace(self, doc):
        d = doc.copy()
        d["_id"] = self._last["_id"] + 1
        self._last = d
        self.in_db = False

    def insert(self):
        try:
            self.col.insert_one(self._last.copy())
        except DuplicateKeyError:
            self.in_db = True
            logging.error("write bar | insert | %s | last bar already exists", self._last)
        except Exception as e:
            logging.error("write bar | insert | %s | %s", self._last, e)
        else:
            self.in_db = True
            logging.info("write bar | insert | %s | ok", self._last)
    
    def update(self, doc):
        if not self.in_db:
            self.insert()
            return
        try:
            self.col.update_one({"datetime": doc["datetime"]}, {"$set": doc})
        except Exception as e:
            logging.error("write bar | update | %s | %s", doc, e)


class MongodbBarAppender(object):

    def __init__(self, container):
        assert isinstance(container, MongoDBCappedCointainer)
        self.container = container
        self.bars = {}
        for name in self.container.names:
            self.bars[name] = BarStorage(self.container[name])
    
    @classmethod
    def config(cls, host, db, names, indexes, **options):
        container = MongoDBCappedCointainer(
            MongoClient(host)[db],
            names, indexes, **options
        )
        return cls(container)

    def put(self, name, doc):
        self.bars[name].put(doc)
    
    def last(self, name):
        return self.bars[name].last