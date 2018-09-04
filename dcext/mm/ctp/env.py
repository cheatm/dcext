import json
import pandas as pd
import logging
import sys
from datetime import time
import os


logging.basicConfig(stream=sys.stdout)


jaqs_addr = "tcp://data.quantos.org:8910"
jaqs_user = ""
jaqs_password = ""

mongodb_uri = "localhost"
mongodb_db = "ctp"
mongodb_map = {}

tick_subscribe = "tcp://127.0.0.1:10001"
tick_publish = "tcp://*:20001"

jqbar_subscribe = "tcp://127.0.0.1:20001"

listen_symbol = set()
listen_freq = {"M1"}

mapper = {}
dates = []
timelimit = {}


def init(*filename, **default):
    conf = dict([name.split("=", 1) for name in filename])
    root = conf.pop("dir", "./etc")
    for key, value in default.items():
        default[key] = os.path.join(root, value)
    default.update(conf)
    load(**default)


def load(config, inst="", calendar="", market=""):
    with open(config) as f:
        conf = json.load(f)
    
    for name in ["jaqs", "mongodb", "tick", "jqbar"]:
        replace(name, conf.get(name, {}))
    
    listen = conf.get("listen", {})
    listen_symbol.update(set(listen.get("symbol", [])))
    listen_freq.update(set(listen.get("freq", [])))

    for name in [
        "jaqs_addr", "jaqs_user", "tick_subscribe", "tick_publish",
        "jqbar_subscribe", "listen_symbol", "listen_freq"
        ]:
        logging.warning("config | %s | %s", name, globals()[name])

    if inst:
        logging.warning("config | instruments file | %s", inst)
        insts = pd.read_csv(inst).set_index("symbol")
        for symbol  in listen_symbol:
            mapper[insts.loc[symbol, "jzcode"]] = symbol 
        if market:
            logging.warning("config | market file | %s", market)
            limits = market_time_limit(market)
            for symbol  in listen_symbol:
                timelimit[symbol] = limits.get(insts.loc[symbol, "market"], tuple())

    if calendar:
        logging.warning("config | calendar | %s", calendar)
        globals()["dates"] = pd.read_csv(calendar)["date"]


def is_trade_time(symbol, dt):
    limits = timelimit.get(symbol, None)
    t = dt.time()
    if limits:
        for begin, end in limits:
            if begin <= t and (t < end):
                return True
        return False
    else:
        return True


def replace(head, conf):
    for key, value in conf.items():
        name = "%s_%s" % (head, key)
        globals()[name] = value


def get_table_name(name, gran):
    mapped = mongodb_map.get(name, name)
    return "%s_%s" % (mapped, gran)


def market_time_limit(filename):
    market = pd.read_csv(filename, index_col="market")
    limits = {}
    for code in market.index:
        l = []
        for i in range(1, 5):
            begin, end = market.loc[code, "auctbeg%d" % i], market.loc[code, "auctend%d" % i]
            if begin < end:
                l.append((split(begin), split(end)))
            elif begin > end:
                l.append((split(begin), time(23, 59, 59, 999999)))
                l.append((time(0, 0, 0), split(end)))
            else:
                break
        limits[code] = tuple(l)
    return limits
            

def split(n):
    return time(int(n/100), n%100) 


def main():
    # r = init(r"config=D:\DataServer\etc\ctp.json", r"inst=D:\DataServer\etc\instrument.csv", r"market=D:\DataServer\etc\market.csv")
    init(config="ctp.json", inst="instrument.csv", market="market.csv")





if __name__ == '__main__':
    main()