from dcext.mm.storage import MongodbBarAppender, MongoDBCappedCointainer
from dcext.mm.message import QuotePublisher, get_sub_sock
from dcext.framework.bars import Handler, Publisher, CoreEngine, VBar1M, VBar1H, NEW, UPD, OLD
from dcext.mm.ctp.proto import make_bar_req, BarResp
from datetime import datetime, timedelta
from dcext.zeromq import get_publish_sock
from dcext.mm.ctp import env
import logging


TICK = 0
BAR = 1


class Tick:

    def __init__(self, symbol, time, price, volume):
        self.symbol = symbol
        self.time = time
        self.price = price
        self.volume = volume


GRANULARITIES = {
    "M1": VBar1M,
    "M3": VBar1M.freq(3),
    "M5": VBar1M.freq(5),
    "M15": VBar1M.freq(15),
    "M30": VBar1M.freq(30),
    "H1": VBar1H,
}


class BarsInstance(object):
    
    granularities = {
        "M1": VBar1M, 
    }

    def __init__(self, granularities):
        self.granularities = granularities
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
            bars = self.bars[tick.symbol]
        except KeyError:
            bars = {gran: cls(tick.time, tick.price, tick.price, tick.price, tick.price, tick.volume, tick.volume) for gran, cls in self.granularities.items()}
            self.bars[tick.symbol] = bars
            return {gran: (NEW, bar.to_dict()) for gran, bar in bars.items()}
        
        results = {}
        for gran, bar in bars.items():
            try:
                results[gran] = bar.on_tick(
                    time=tick.time,
                    price=tick.price,
                    volume=tick.volume
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


class BarsStorage(Handler):

    gran_diff = {
        "M1": timedelta(),
        "M3": timedelta(minutes=2),
        "M5": timedelta(minutes=4),
        "M15": timedelta(minutes=14),
        "M30": timedelta(minutes=29),
        "H1": timedelta(minutes=59),
    }

    def __init__(self, storage, addr, bars):
        assert isinstance(storage, MongodbBarAppender)
        assert isinstance(bars, BarsInstance)
        self.bars = bars
        self.storage = storage
        self.addr = addr
        self.sock = get_publish_sock(addr)
        self.sock.send(b"start")
        self.methods = {
            NEW: self.handle_new,
            UPD: self.handle_upd,
            OLD: lambda symbol, gran, data, time:None,
        }
    
    def handle_new(self, symbol, gran, data, time):
        name = env.get_table_name(symbol, gran)
        self.storage.finish(name)
        if env.is_trade_time(symbol, time):
            doc = transform(data)
            doc["flag"] = 0
            self.storage.put(name, doc)
            logging.warning("write | new bar | %s | %s | %s", symbol, gran, data)

        end = data["datetime"]
        start = end - self.gran_diff[gran]
        self.send_bar_req(symbol, start.hour*10000+start.minute*100, end.hour*10000+end.minute*100, freq=gran)

    def handle_upd(self, symbol, gran, data, time):
        name = env.get_table_name(symbol, gran)
        if env.is_trade_time(symbol, time):
            doc = transform(data)
            self.storage.put(name, doc)

    def handle(self, tick):
        for gran, change in self.bars.on_tick(tick).items():
            tag, data = change
            method = self.methods[tag]
            method(tick.symbol, gran, data, tick.time)
    
    def send_bar_req(self, symbol, start, end, freq):
        req = make_bar_req(symbol, start, end, freq=freq)
        self.sock.send(req)


def transform(bar):
    doc = bar.copy()
    doc["datetime"] = doc["datetime"].strftime("%Y%m%d %H:%M:%S")
    return doc


def make_dt(date, time):
    y = int(date/10000)
    d = date % 100
    m = int((date % 10000 - d)/100)

    ms = time%1000
    t = int(time/1000)
    h = int(t/3600)
    _m = t % 3600
    s = _m % 60
    _m = int(_m/60)
    return datetime(y, m, d, h, _m, s, ms*1000)


class TickPublisher(QuotePublisher):

    def __init__(self, addr, mapper):
        super(TickPublisher, self).__init__(addr)
        self.mapper = mapper

    def __iter__(self):
        for quote in super(TickPublisher, self).__iter__():
            symbol = self.mapper.get(quote.jzcode, None)
            if symbol:
                tick = Tick(
                    symbol,
                    make_dt( quote.qs.tradeday, quote.time),
                    quote.last, quote.volume
                )
                yield TICK, tick


class BarReceiver(Publisher):

    columns = ["open", "high", "low", "close", "volume"]

    def __init__(self, addr):
        self.addr = addr
        self.sock = get_sub_sock(addr)
    
    def __iter__(self):
        while True:
            try:
                msg = self.sock.recv_multipart()[0]
                rsp = self.parse(msg)
            except Exception as e:
                logging.error("rec bars | %s", e)
            else:
                for bar in rsp.bars:
                    doc = {key: getattr(bar, key) for key in self.columns}
                    doc["datetime"] = datetime.strptime("%06dT%06d" % (bar.date, bar.time), "%Y%m%dT%H%M%S") - timedelta(minutes=1)
                    yield BAR, doc

    @staticmethod
    def parse(msg):
        rsp = BarResp()
        rsp.ParseFromString(msg)
        return rsp


def run():
    from itertools import product

    qp = TickPublisher(env.tick_subscribe, env.mapper)
    appender = MongodbBarAppender.config(
        env.mongodb_uri, env.mongodb_db,
        [env.get_table_name(symbol, gran) for symbol, gran in product(env.listen_symbol, env.listen_freq)],
        ["datetime"], size=2**25, max=1000
    )

    bars = BarsInstance({freq: GRANULARITIES[freq] for freq in env.listen_freq})
    handler = BarsStorage(appender, env.tick_publish, bars)
    core = CoreEngine()
    core.register_publisher("qp", qp)
    core.register_handler(TICK, handler)
    core.start()


def main():
    import sys
    env.init(*sys.argv[1:], config="ctp.json", inst="instrument.csv", market="market.csv")
    run()
 

if __name__ == '__main__':
    main()
