from dcext.framework.transmit import Stream, LOG, MESSAGE
import logging
from dcext.oanda import api
import json


class OandaPriceStream(Stream):

    TYPES = {"PRICE": MESSAGE, "HEARTBEAT": LOG}

    def __init__(self, api, instruments, accountID):
        self.instruments = instruments
        self.api = api
        self.accountID = accountID

    def get_accountID(self):
        if self.accountID:
            return self.accountID
        else:
            content = self.api.get(api.ACCUOUNTS)
            doc = json.loads(content)
            self.accountID = doc["accounts"][0]["id"]
            return self.accountID

    def __iter__(self):
        accountID = self.get_accountID()
        instruments = ",".join(self.instruments) if isinstance(self.instruments, list) else self.instruments
        for content in self.api.stream(api.PRICING_STREAM, accountID=accountID, instruments=instruments):
            try:
                doc = json.loads(content)
                _type = self.TYPES.get(doc["type"], LOG)
            except:
                pass
            else:
                yield _type, doc


CANDLE_TAG = "CANDLE"
CANDLE_SUC = 1
CANDLE_FAIL = 0


class OandaCandleUpdater(object):

    NAME_MAP = {
        "openMid": "open",
        "highMid": "high", 
        "closeMid": "close",
        "lowMid": "low",
        "volume": "volume"
    }

    def __init__(self, api, instruments):
        self.api = api
        self.instruments = instruments
        self.query = {
            "count": 2, "granularity": "D", "candleFormat": "midpoint"
        }

    def _get(self, instrument):
        content = self.api.get(api.CANDLES, self.query, instrument=instrument)
        doc = json.loads(content)

        candle = doc["candles"][1]
        result = {value: candle[key] for key, value in self.NAME_MAP.items()}
        result["instrument"] = doc["instrument"]
        result["preclose"] = doc["candles"][0]["closeMid"]
        return result

    def __iter__(self):
        for instrument in self.instruments:
            try:
                yield CANDLE_SUC, self._get(instrument)
            except Exception as e:
                logging.error("Query oanda candle | %s | %s", instrument, e)
                yield CANDLE_FAIL, {"instrument": instrument, "error": str(e), "operate": "Query candle"}

from threading import Thread
from queue import Queue, Empty
import time



class OandaStream(Stream):

    def __init__(self, stream, candle, sleep=10):
        assert isinstance(stream, OandaPriceStream)
        assert isinstance(candle, OandaCandleUpdater)
        self.stream = stream
        self.candle = candle
        self.cache = {}
        self.sleep = sleep
        self.running = False
        self.queue = Queue()
        self.price_thread = Thread(target=self.price_stream, daemon=True)
        self.candle_thread = Thread(target=self.candle_stream, daemon=True)
        self.methods = {
            CANDLE_TAG: self.on_candle,
            MESSAGE: self.on_price,
            LOG: self.on_log
        }
    
    @classmethod
    def conf(cls, token, instruments, accountID=None, sleep=10, trade_type=api.PRACTICE):
        assert isinstance(instruments, list)
        _api = api.OandaAPI(token, trade_type=trade_type)
        stream = OandaPriceStream(_api, instruments, accountID)
        candle = OandaCandleUpdater(_api, instruments)
        return cls(stream, candle, sleep)

    def price_stream(self):
        stream = self.stream.__iter__()
        while self.running:
            messages = next(stream)
            self.queue.put(messages)
    
    def candle_stream(self):
        while self.running:
            for tag, candle in self.candle:
                if tag == CANDLE_SUC:
                    self.queue.put((CANDLE_TAG, candle))
                else:
                    self.queue.put((LOG, candle))
            time.sleep(self.sleep)

    def on_price(self, price):
        instrument = price["instrument"]
        candle = self.cache.get(instrument)
        if candle:
            # return make_ind(price, candle)
            return {"price": price, "candle": candle}

    def on_candle(self, candle):
        instrument = candle["instrument"]
        self.cache[instrument] = candle
    
    def on_log(self, log):
        return log

    def start(self):
        self.running = True
        if self.price_thread is None:
            self.price_thread = Thread(target=self.price_stream, daemon=True)
        if not self.price_thread.is_alive():
            self.price_thread.start()

        if self.candle_thread is None:
            self.candle_thread = Thread(target=self.candle_stream, daemon=True)
        if not self.candle_thread.is_alive():
            self.candle_thread.start()

    def stop(self):
        self.running = False
        self.join()
        
    def join(self):
        if self.price_thread and self.price_thread.is_alive():
            self.price_thread.join()
        if self.candle_thread and self.candle_thread.is_alive():
            self.candle_thread.join()
        
    def __iter__(self):
        if not self.running:
            self.start()
        while self.running or self.queue.qsize():
            try:
                TAG, data = self.queue.get(timeout=2)
            except Empty:
                continue

            try:
                method = self.methods[TAG]
                result = method(data)
            except Exception as e:
                logging.error("Oanda handle | %s | %s | %s", TAG, data, e)
            else:
                if result:
                    yield TAG, result
            

from dcext.proto.md_pb2 import MarketDataInd, MarketQuote, AskBid, QuoteStatic, MD_FUT_L1
from datetime import datetime
from dcext import instruments


FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
PRICES = ["open", "high", "low", "close", 'volume']
PRICE = "price"
LIQUIDITY = "liquidity"
ZERO_QUOTE = ["turnover", "interest", "settle", "delta", "iopv", "avgbidpx", "totbidvol", "avgaskpx", "totaskvol", "vwap"]
ZERO_QS = ["uplimit", "downlimit", "preinterest", "preclose", "presettle", "predelta"]


def make_ind(price, candle):
    ind = MarketDataInd()
    ind.type = MD_FUT_L1
    quote = ind.fut
    ab = quote.ab
    qs = quote.qs

    fill_zero(qs, ZERO_QS)
    fill_zero(quote, ZERO_QUOTE)

    askbid(ab.askPrice, ab.askVolume, price["asks"])
    askbid(ab.bidPrice, ab.bidVolume, price["bids"])

    dt = datetime.strptime(price["time"][:26], FORMAT)
    date = dt.year*10000+dt.month*100+dt.day
    millionsecond = (dt.second*3600+dt.minute*60+dt.second)*1000+int(dt.microsecond/1000)
    
    qs.date = date
    qs.tradeday=date
    
    quote.time = millionsecond
    quote.jzcode = instruments.jzcode(price["instrument"])
    for name in PRICES:
        setattr(quote, name, candle[name])
    quote.last = quote.close
    return ind


def askbid(price, volume, pairs):
    for doc in pairs:
        price.append(float(doc[PRICE]))
        volume.append(doc[LIQUIDITY])


def fill_zero(proto, names):
    for name in names:
        setattr(proto, name, 0)


def main():
    instruments.init()
    pos = OandaStream.conf(api.TOKEN, ["EUR_USD", "AUD_JPY"])
    pos.start()
    count = 0
    for tag, data in pos:
        
        print(tag, data)
        count += 1
        if count > 5:
            pos.stop()