from dcext.mm.ctp.run import MongodbBarAppender
from dcext.mm.ctp import env
from dcext.mm.ctp.jqbar import make_time
from jaqs.data import DataApi
import pandas as pd
from datetime import datetime, timedelta
import logging


def history(host, db, symbols, api, trade_days, max=1000):
    assert isinstance(api, DataApi)
    storage = MongodbBarAppender.config(
        host, db, 
        [env.get_table_name(name, "M1") for name in symbols],
        ["datetime"], size=2**25, max=max
    )

    for symbol in symbols:
        table = env.get_table_name(symbol, "M1")
        last = storage.last(table)
        if last:
            dt = datetime.strptime(last["datetime"], "%Y%m%d %H:%M:%S")
            dates = find(trade_days, date2int(datetime.now()), date2int(dt))
            data = create(api, symbol, dates, max)
        else:
            dates = find(trade_days, date2int(datetime.now()))
            data = create(api, symbol, dates, max)
        data["flag"] = 1
        for doc in data.to_dict("record"):
            doc["datetime"] = doc["datetime"].strftime("%Y%m%d %H:%M:%S")
            doc["volume"] = int(doc["volume"])
            storage.put(table, doc)
        
        logging.warning("reload | %s", symbol)
    

def date2int(date):
    return date.year * 10000 + date.month * 100 + date.day


def now_date():
    dt = datetime.now()
    return dt.year * 10000 + dt.month * 100 + dt.day


def create(api, symbol, trade_days, max_length):
    length = 0
    bar_list = []
    for bar in bars(api, symbol, trade_days):
        length += len(bar.index)
        bar_list.insert(0, bar)
        if length >= max_length:
            break
    data = pd.concat(bar_list, ignore_index=True)
    data["datetime"] = list(map(make_time, data["date"], data['time']))
    return data[["datetime", "open", "high", "low", "close", "volume"]]



def bars(api, symbol, trade_days):
    assert isinstance(api, DataApi)
    for date in reversed(trade_days):
        data, msg = api.bar(symbol, trade_date=date)
        if msg == "0,":
            yield data
        else:
            raise Exception(msg)


def find(trade_days, date, begin=0):
    hist = []
    iterable = iter(trade_days)
    for d in iterable:
        if d >= begin:
            if date >= d:
                hist.append(d)
            break

    for d in iterable:
        if date >= d:
            hist.append(d)
        else:
            break
    return hist


def get_today_int():
    dt = datetime.now()
    return dt.year*10000 + dt.month*100 + dt.day


def run():
    api = DataApi(env.jaqs_addr)
    api.login(env.jaqs_user, env.jaqs_password)
    history(
        env.mongodb_uri, env.mongodb_db,
        env.listen_symbol, api, env.dates
    )


def main():
    import sys
    env.init(*sys.argv[1:], config="ctp.json", calendar="calendar.csv")
    run()


if __name__ == '__main__':
    main() 