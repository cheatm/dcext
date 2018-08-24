from dcext.framework.bars import Handler, Publisher, CoreEngine, Bar1M, NEW, UPD, OLD
from dcext.oanda.stream import OandaPriceStream, OandaCandleUpdater, LOG, MESSAGE
from dcext.oanda.api import TOKEN, OandaAPI
from dcext.oanda.api import CANDLES as CANDLES_URL
from datetime import datetime, timedelta, timezone
from pymongo.database import Collection, Database
from pymongo.errors import DuplicateKeyError
from pymongo import MongoClient
from queue import Queue, Empty
import json
import time
import logging


TICK_STREAM = 0
REQ_BAR = 1

INSTRUMENT = "instrument"
GRANULARITY = "granularity"
COUNT = "count"
LASTPRICE = "price"


class EmptyHandler(Handler):

    def handle(self, data):
        print(data)


class OandaStreamPublisher(Publisher):

    DATE_FMT = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, stream, tz=0):
        assert isinstance(stream, OandaPriceStream)
        self.stream = stream
        self.tz = timezone(timedelta(hours=tz))
    
    def __iter__(self):
        for _type, _msg in self.stream:
            if _type == MESSAGE:
                try:
                    message = self.decorate(_msg)
                except Exception as e:
                    logging.error("oanda stream | %s | %s", _msg, e)
                else:
                    yield TICK_STREAM, message
            else:
                logging.warning("oanda stream log | %s", _msg)

    def decorate(self, msg):
        doc = {}
        doc["time"] = datetime.strptime(
            msg["time"].split(".")[0],
            self.DATE_FMT
        ).replace(tzinfo=self.tz)
        doc[INSTRUMENT] = msg[INSTRUMENT]
        ask = self.get_price(msg["asks"])
        bid = self.get_price(msg["bids"])
        doc[LASTPRICE] = (ask+bid)/2
        return doc

    @staticmethod
    def get_price(prices):
        return float(prices[0]["price"])


class OandaCandlePublisher(Publisher):

    REQ_DATETIME_FORMAT = "%Y-%m-%dT%H%%3A%M%%3A%SZ"
    RSP_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    REPLACEMENTS = {
        "openMid": "open",
        "highMid": "high",
        "lowMid": "low",
        "closeMid": "close"
    }

    def __init__(self, api, queue, tz=0):
        assert isinstance(api, OandaAPI)
        self.api = api
        self.queue = queue
        self.tz = timezone(timedelta(hours=tz))

    def get(self, instrument, granularity="M1", count=1000, start=None, **kwargs):
        query = {
            GRANULARITY: granularity, "candleFormat": "midpoint"
        }
        if isinstance(start, datetime):
            if count == 1:
                query[COUNT] = 1
                query["start"] = start.astimezone(self.tz).strftime(self.REQ_DATETIME_FORMAT) 
            else:
                now = datetime.now(self.tz)
                if now - start >= timedelta(minutes=1000):
                    query[COUNT] = 1000
                else:
                    query["start"] = start.astimezone(self.tz).strftime(self.REQ_DATETIME_FORMAT) 
        elif not start:
            query[COUNT] = count               
        
        logging.warning("reload | %s", query)
        content = self.api.get(CANDLES_URL, query, instrument=instrument)
        doc = json.loads(content)
        self.decorate(doc)
        return doc
    
    def decorate(self, doc):
        for bar in doc["candles"]:
            try:
                bar["datetime"] = datetime.strptime(bar.pop("time"), self.RSP_DATETIME_FORMAT).replace(tzinfo=self.tz)
                for o, u in self.REPLACEMENTS.items():
                    bar[u] = bar.pop(o)
            except: 
                pass

    def __iter__(self):
        while True:
            try:
                req = self.queue.get(timeout=1)
            except Empty:
                continue
            
            try:
                doc = self.get(**req)
            except Exception as e:
                logging.error("request bar error | %s | %s", req, e)
            else:
                yield REQ_BAR, doc


class BarsInstance(object):
    
    granularities = {
        "M1": Bar1M
    }

    def __init__(self):
        self.bars = {}
    
    @staticmethod
    def split_key(key):
        if isinstance(key, str):
            return key.rsplit("_", 1)
        elif isinstance(key, tuple):
            return key[0], key[1]
        else:
            raise ValueError("Invalid key pattern.")

    def on_tick(self, tick):
        try:
            name = tick[INSTRUMENT]
            bars = self.bars[name]
        except KeyError:
            return {}
        results = {}
        for gran, bar in bars.items():
            try:
                results[gran] = bar.on_tick(
                    time=tick["time"],
                    price=tick[LASTPRICE]
                )
            except Exception as e:
                logging.error("transform tick | %s | %s", tick, e)
        return results
            

    def __getitem__(self, key):
        inst, gran = self.split_key(key)
        return self.bars[inst][gran]
    
    def __setitem__(self, key, value):
        inst, gran = self.split_key(key)
        cls = self.granularities[gran]
        if isinstance(value, dict):
            cls = self.granularities[gran]
            bar = cls(**value)
        elif isinstance(value, cls):
            bar = value
        else:
            raise ValueError("Invalid value. Only dict and %s expected" % cls)
        self.bars.setdefault(inst, {})[gran] = bar


from pymongo.database import Collection, Database


class MongoDBHandler(Handler):

    DATETIME_FORMAT = "%Y%m%d %H:%M:%S"

    def __init__(self, bars, db, queue, *tables, tz=0):
        assert isinstance(bars, BarsInstance)
        assert isinstance(db, Database)
        self.bars = bars
        self.db = db    
        self.queue = queue
        all_table = self.db.collection_names()
        self.tz = timezone(timedelta(hours=tz))
        for inst, gran in tables:
            name = self.table_name(inst, gran)   
            if name not in all_table:
                collection = self.db.create_collection(
                    name, capped=True, size=2**25, max=1000
                )
                collection.create_index("datetime", unique=True)
        self.tables = tables
    
    def transform(self, bar):
        bar["datetime"] = bar["datetime"].strftime(self.DATETIME_FORMAT)

    @staticmethod
    def table_name(inst, gran):
        return "%s_%s" % (inst.replace("_", ""), gran)
    
    def get_collection(self, inst, gran):
        return self.db[self.table_name(inst, gran)]

    def insert(self, col, bar):
        try:
            col.insert_one(bar)
        except DuplicateKeyError:
            pass

    def upsert(self, col, bar):
        col.update_one({"datetime": bar["datetime"]}, {"$set": bar}, upsert=True)

    def update(self, col, bar):
        col.update_one({"datetime": bar["datetime"]}, {"$set": bar})



class MongoDBBarHandler(MongoDBHandler):

    PROJECTION = {
        "datetime": 1,
        "open": 1,
        "high": 1,
        "close": 1,
        "low": 1,
        "volume": 1,
        "_id": 0
    }
    
    def __init__(self, bars, db, queue, *tables, tz=0):
        super(MongoDBBarHandler, self).__init__(bars, db, queue, *tables, tz=tz)
        self.load_bars()
    
    def handle(self, data):
        inst, gran = data[INSTRUMENT], data[GRANULARITY]
        collection = self.get_collection(inst, gran)
        for bar in data["candles"]:
            complete = bar.pop("complete")
            if not complete:
                self.bars[inst, gran] = bar
            self.transform(bar)
            logging.warning("write bar | %s | %s | %s", inst, gran, bar)
            self.upsert(collection, bar)
    
    def load_bars(self):
        for inst, gran in self.tables:
            self.get_bar(inst, gran)

    def get_bar(self, inst, gran):
        doc = self.get_collection(inst, gran).find_one(projection=self.PROJECTION, sort=[("datetime", -1)])
        req = {
            INSTRUMENT: inst,
            GRANULARITY: gran
        }
        if doc:
            t = doc["datetime"]
            doc["datetime"] = datetime.strptime(t, self.DATETIME_FORMAT).replace(tzinfo=self.tz)
            self.bars[inst, gran] = doc
            req["start"] = doc["datetime"]
        else:
            req[COUNT] = 1000
        self.queue.put(req)


class MongoDBTickHandler(MongoDBHandler):
    
    def __init__(self, bars, db, queue, *tables, tz=0):
        super(MongoDBTickHandler, self).__init__(bars, db, queue, *tables, tz=tz)
        self.methods = {
            NEW: self.new_candle,
            UPD: self.update_candle,
            OLD: lambda inst, gran, data: None
        }
    
    def handle(self, tick):
        inst = tick[INSTRUMENT]
        operates = self.bars.on_tick(tick)
        for gran, results in operates.items():
            op, data = results
            method = self.methods[op]
            method(inst, gran, data)
    
    def new_candle(self, inst, gran, data):
        date = data["datetime"] - timedelta(minutes=1)
        self.transform(data)
        self.insert(
            self.get_collection(inst, gran),
            data
        )
        self.queue.put({INSTRUMENT: inst, GRANULARITY: gran, COUNT: 1, "start": date})
        logging.warning("new bar | %s | %s | %s", inst, gran, data)
    
    def update_candle(self, inst, gran, data):
        self.transform(data)
        self.update(
            self.get_collection(inst, gran),
            data
        )
            

def command(config_file):
    import json
    config = json.load(open(config_file))
    
    # run(["XAU_USD", "EUR_USD"], ["M1"], "192.168.0.105:37017", "oanda")
    run(**config)


def run(instrument, granularity, mongodb_uri, db_name):
    from itertools import product
    instruments = instrument
    inst_grans = list(product(instruments, granularity))

    api = OandaAPI(TOKEN)
    q = Queue()
    bars = BarsInstance()
    db = MongoClient(mongodb_uri)[db_name]
    osp = OandaStreamPublisher(OandaPriceStream(api, ["XAU_USD", "EUR_USD"]))
    ocp = OandaCandlePublisher(api, q)
    mbh = MongoDBBarHandler(bars, db, q, *inst_grans)
    mth = MongoDBTickHandler(bars, db, q, *inst_grans)
    
    core = CoreEngine()
    core.register_publisher("osp", osp)
    core.register_publisher("ocp", ocp)
    core.register_handler(TICK_STREAM, mth)
    core.register_handler(REQ_BAR, mbh)
    core.start()


def main():
    import sys
    try:
        config_file = sys.argv[1]
    except:
        config_file = "etc/oanda.json"
    command(config_file)


if __name__ == '__main__':
    main()