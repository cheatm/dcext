#! encoding:utf-8
from dcext.framework.bars import Publisher, Handler, CoreEngine
from dcext.mm.ctp.hist import get_bar
from dcext.zeromq import get_publish_sock, subscribe
from jaqs.data import DataApi
from pymongo.database import Database
from datetime import datetime, timedelta
from dcext.mm.ctp import proto
from dcext.mm.ctp import env
import pandas as pd
import logging
import json


REQUEST = 0


METHOD = "_m"
PARAMS = "_p"

KEYS = {METHOD, PARAMS}

COLUMNS = ["date", "time", "open", "high", "low", "close", "volume"]
STORAGE_COLUMNS = ["open", "high", "low", "close", "volume", "datetime"]


class RequestReceiver(Publisher):

    def __init__(self, addr):
        self.addr = addr
    
    def __iter__(self):
        for msg in subscribe(self.addr):
            req = self.parse(msg)
            if req:
                params = {
                    "symbol": req.symbol, 
                    "start_time": req.start_time, 
                    "end_time": req.end_time,
                    "trade_date": req.trade_date,
                    "freq": req.freq
                }
                yield REQUEST, params
    
    @staticmethod
    def parse(msg):
        req = proto.BarReq()
        try:
            req.ParseFromString(msg)
        except:
            pass
        else:
            return req
            

class ReqestHandler(Handler):

    def __init__(self, storage, user, password, jaqs_addr="tcp://data.quantos.org:8910"):
        self.user = user
        self.password = password
        self.jaqs_addr = jaqs_addr
        self.api = DataApi(self.jaqs_addr)
        assert isinstance(storage, BarStorage)
        self.storage = storage
    
    def __enter__(self):
        self.api.login(self.user, self.password)
        return self.api
    
    def __exit__(self, *args):
        self.api.logout()
    
    def handle(self, req):
        logging.warning("received bar req | %s", req)
        with self as api:
            try:
                data = get_bar(api, **req)
            except Exception as e:
                logging.error("require bar error | %s", e)
            else:
                self.send(
                    env.get_table_name(req["symbol"], req["freq"]),
                    data
                )
            
        # with self as api:
        #     data, msg = getattr(api, method)(**params)
        #     data["flag"] = 1
        #     if msg == "0,":
        #         self.send(params["symbol"], data)
        #     else:
        #         logging.error("require bar error | %s", msg)
    
    def send(self, symbol, data):
        for doc in data[STORAGE_COLUMNS].to_dict("record"):
            doc["volume"] = int(doc["volume"])
            doc["flag"] = 1
            self.storage.put(symbol, doc)
            logging.warning("update bar | %s | %s", symbol, doc)

    @staticmethod
    def make_rsp(symbol, data):
        assert isinstance(data, pd.DataFrame)
        bars = []
        for doc in data[COLUMNS].to_dict("record"):
            doc["date"] = int(doc["date"])
            doc["time"] = int(doc["time"])
            doc["volume"] = int(doc["volume"])
            bars.append(proto.make_msg(proto.BAR, **doc))
        rsp = proto.make_msg(proto.BAR_RESP, symbol=symbol)
        rsp.bars.extend(bars)
        return rsp


class BarStorage(object):
    
    def __init__(self, db):
        self.db = db
    
    @classmethod
    def config(cls, host, db):
        from pymongo import MongoClient
        return cls(MongoClient(host)[db])

    def put(self, name, doc):
        dt = doc.pop("datetime").strftime("%Y%m%d %H:%M:%S")
        try:
            self.db[name].update_one({"datetime": dt}, {"$set": doc})
        except Exception as e:
            logging.error("write db | %s | %s | %s | %s", name, dt, doc, e)
    


def make_time(date, time):
    day, month, year = tuple(split(date))
    second, minute, hour = tuple(split(time))
    return datetime(year, month, day, hour, minute, second) - timedelta(minutes=1)



def split(num, d=100, left=3):
    
    while num >= d and (left > 1):
        yield num % d
        num = int(num/d)
        left -= 1
    else:
        for i in range(left):
            yield num
            num = 0
    
    
def make_bar_req(symbol, start_time=200000, end_time=160000, trade_date=0):
    msg = proto.make_msg(proto.BAR_REQ, symbol=symbol, start_time=start_time, end_time=end_time, trade_date=trade_date)
    return msg.SerializeToString()


def test():
    sub_addr = "tcp://127.0.0.1:20009"
    pub_addr = "tcp://*:20010"
    publisher = RequestReceiver(sub_addr)
    storage = BarStorage.config("192.168.0.105:37017", "dummy_tcp")
    handler = ReqestHandler(storage, "18566262672", "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MTI3MDI3NTAyMTIiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTg1NjYyNjI2NzIifQ.O_-yR0zYagrLRvPbggnru1Rapk4kiyAzcwYt2a3vlpM")
    core = CoreEngine()
    core.register_publisher("req", publisher)
    core.register_handler(REQUEST, handler)
    core.start()


def run():
    publisher = RequestReceiver(env.jqbar_subscribe)
    storage = BarStorage.config(env.mongodb_uri, env.mongodb_db)
    handler = ReqestHandler(storage, env.jaqs_user, env.jaqs_password, env.jaqs_addr)
    core = CoreEngine()
    core.register_publisher("req", publisher)
    core.register_handler(REQUEST, handler)
    core.start()


def main():
    import sys
    env.init(*sys.argv[1:], config="ctp.json")
    run()


if __name__ == '__main__':
    main()
        

