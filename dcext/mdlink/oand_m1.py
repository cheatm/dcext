from dcext.oanda.api import OandaAPI, CANDLESV3, TOKEN
from datetime import datetime, timedelta, timezone
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from itertools import product
import logging
import json


def get_dt(date, tz=None):
    if isinstance(date, int):
        return get_dt(str(date), tz)
    elif isinstance(date, str):
        return datetime.strptime(date.replace("-", ""), "%Y%m%d").replace(tzinfo=tz)
    elif isinstance(date, datetime):
        return date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=tz)
    else:
        raise TypeError("Not supported type: %s" % type(date))



class API(OandaAPI):

    def bar(self, instrument, granularity, start, end):
        if isinstance(start, datetime):
            start = start.timestamp()
        if isinstance(end, datetime):
            end = end.timestamp()
        query = {
            "granularity": granularity,
            "from": start,
            "to": end
        }
        content = self.get(CANDLESV3, query, instrument=instrument)
        data = json.loads(content)

        result = []
        for bar in data["candles"]:
            result.append(self.generate(bar))
        return  result
    
    MAPPER = {"o": "open", "h": "high", "c": "close", "l": "low"}

    def generate(self, bar):
        doc = bar.copy()
        mid = doc.pop("mid")
        for o, t in self.MAPPER.items():
            doc[t] = mid[o]
        return doc


class MongodbStorage(object):

    INSTRUMENT = "_i"
    START = "_s"
    END = "_e"
    DATE = "_d"
    COUNT = "_c"
    FILL = "_f"
    MODIFY = "_m"

    def __init__(self, host=None, db="OANDA_M1", log="log.oanda", tz=0):
        self.client = MongoClient(host)
        self.db = self.client[db]
        ldb, lcol = log.split(".", 1)
        self.log = self.client[ldb][lcol]
        self.tz = timezone(timedelta(hours=tz))
        self.init_log_collection()

    def ensure_table(self, instrument):
        self.db[instrument].create_index("datetime", unique=1, background=True)
    
    def init_log_collection(self):
        self.log.create_index([
            (self.INSTRUMENT, 1),
            (self.DATE, 1)
        ], unique=True, background=True)
    
    def create(self, instrument, date):
        dt = get_dt(date, self.tz)
        filters = {
            self.INSTRUMENT: instrument,
            self.DATE: date,
        }
        doc = {
            self.START: dt,
            self.END: dt+timedelta(days=1),
            self.COUNT: 0,
            self.FILL: 0,
            self.MODIFY: datetime.now()
        }
        doc.update(filters)
        return self.log.update_one(filters, {"$setOnInsert": doc}, upsert=True).upserted_id
    
    def fill(self, instrument, date, count, fill):
        filters = {
            self.INSTRUMENT: instrument,
            self.DATE: date
        }
        doc = {self.COUNT: count, self.FILL: fill, self.MODIFY: datetime.now()}
        self.log.update_one(filters, {"$set": doc})

    def find(self, instruments=None, start=None, end=None, filled=False):
        filters = {}
        if instruments:
            filters[self.INSTRUMENT] = {"$in": instruments}
        if start:
            filters[self.DATE] = {"$gte": start}
        if end:
            filters.setdefault(self.DATE, {})["$lte"] = end
        if filled:
            filters[self.FILL] = {"$gte": 0}
        elif filled is not None:
            filters[self.FILL] = 0
        
        cursor = self.log.find(filters, [self.INSTRUMENT, self.DATE, self.START, self.END])
        for doc in list(cursor):
            yield doc[self.INSTRUMENT], doc[self.DATE], doc[self.START].replace(tzinfo=self.tz), doc[self.END].replace(tzinfo=self.tz)

    def time(self, date):
        if isinstance(date, int):
            return self.time(str(date))
        elif isinstance(date, str):
            return datetime.strptime(date.replace("-", ""), "%Y%m%d").replace(tzinfo=self.tz)
        elif isinstance(datetime, date):
            return date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=self.tz)
    
    def write(self, instrument, data):
        count = 0
        collection = self.db[instrument]
        for bar in data:
            doc = self.vnpy_format(bar, instrument)
            count += self.append(collection, doc)
        return count

    @staticmethod
    def append(collection, bar):
        try:
            collection.insert_one(bar)
        except DuplicateKeyError:
            return 0
        else:
            return 1

    def vnpy_format(self, bar, symbol):
        bar.pop("complete", None)
        bar["symbol"] = symbol
        bar["exchange"] = "oanda"
        bar["vtSymbol"] = "%s:%s" % (symbol, "oanda")
        dt = datetime.strptime(bar.pop("time").split(".")[0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=self.tz)
        bar["datetime"] = dt
        bar["date"] = dt.strftime("%Y%m%d")
        bar["time"] = dt.strftime("%H:%M:%S.000000")
        bar["rawData"] = None
        bar["openInterest"] = 0
        return bar


class Framework(object):

    def __init__(self, api, storage, ltz=8):
        assert isinstance(api, API)
        assert isinstance(storage, MongodbStorage)
        self.api = api
        self.storage = storage
        self.ltz = timezone(timedelta(hours=ltz)) 
    
    def create(self, instruments, start, end):
        dates = pd.date_range(get_dt(start), get_dt(end)).map(lambda t: t.year*10000+t.month*100+t.day)
        for i, d in product(instruments, dates):
            r = self.storage.create(i, d)
            logging.warning("create log | %s | %s | %s", i, d, r)
    
    def publish(self, instruments=None, start=None, end=None, filled=False, redo=3):
        logging.warning("publish cycle start| %s | %s | %s | %s | %s", instruments, start, end, filled, redo)
        now = datetime.now(self.ltz)
        missions = list(self.storage.find(instruments, start, end, filled))
        total = len(missions)
        accomplish = 0
        for i, d, s, e in missions:
            if e >= now:
                logging.warning("publish | %s | %s | end: %s is future", i, d, e)
                accomplish += 1
                continue
            accomplish += self.download(i, d, s, e)
        logging.warning("publish cycle done | total: %s | accomplished: %s", total, accomplish)
        if redo:
            if accomplish < total:
                self.publish(instruments, start, end, False, redo-1)
            
    def download(self, instrument, date, start, end):
        try:
            data = self.api.bar(instrument, "M1", start, end)
            count = len(data)
        except Exception as e:
            logging.error("req bar | %s | %s | %s", instrument, date, e)
            return 0
        
        
        try:
            if count:
                fill = self.storage.write(instrument, data)
            else:
                fill = -1
        except Exception as e:
            logging.error("write bar | %s | %s | %s", instrument, date, e)
            return 0
        
        try:
            self.storage.fill(instrument, date, count, fill)
        except Exception as e:
            logging.error("fill log | %s | %s | %s", instrument, date, e)
        else:
            logging.warning("download bar | %s | %s | fill: %s, count: %s", instrument, date, fill, count)
            return 1


def test():
    symbols = ["EUR_USD", "USD_JPY", "AUD_USD", "USD_CAD", "GBP_USD", "USD_CNH", "HK33_HKD", "NAS100_USD", "XAU_USD", "WTICO_USD"]
    start = 20170101
    end = 20180825
    api = API()
    storage = MongodbStorage("192.168.0.104,192.168.0.105")
    fw = Framework(api, storage)
    
    # fw.create(symbols, start, end)
    fw.publish(redo=-1)


if __name__ == '__main__':
    test()
