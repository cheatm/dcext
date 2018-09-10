from dcext.mm.ctp.run import MongodbBarAppender
from dcext.mm.ctp import env
from jaqs.data import DataApi
import pandas as pd
from datetime import datetime, timedelta
from itertools import product
import logging


def history(host, db, symbols, api, trade_days, freqs, max=1000):
    assert isinstance(api, DataApi)
    storage = MongodbBarAppender.config(
        host, db, 
        [env.get_table_name(name, freq) for name, freq in product(symbols, freqs)],
        ["datetime"], size=2**25, max=max
    )

    for symbol, freq in product(symbols, freqs):
        table = env.get_table_name(symbol, freq)
        last = storage.last(table)
        if last:
            dt = datetime.strptime(last["datetime"], "%Y%m%d %H:%M:%S")
            dates = find(trade_days, date2int(datetime.now()), date2int(dt))
            data = create(api, symbol, dates, freq, max)
        else:
            dates = find(trade_days, date2int(datetime.now()))
            data = create(api, symbol, dates, freq, max)
        data["flag"] = 1
        for doc in data.to_dict("record"):
            doc["datetime"] = doc["datetime"].strftime("%Y%m%d %H:%M:%S")
            doc["volume"] = int(doc["volume"])
            storage.put(table, doc)
        
        logging.warning("reload | %s | %s", symbol, freq)
    

def date2int(date):
    return date.year * 10000 + date.month * 100 + date.day


def now_date():
    dt = datetime.now()
    return dt.year * 10000 + dt.month * 100 + dt.day


def make_time(date, time):
    day, month, year = tuple(split(date))
    second, minute, hour = tuple(split(time))
    return datetime(year, month, day, hour, minute, second)


def split(num, d=100, left=3):
    
    while num >= d and (left > 1):
        yield num % d
        num = int(num/d)
        left -= 1
    else:
        for i in range(left):
            yield num
            num = 0


def create(api, symbol, trade_days, freq, max_length):
    length = 0
    bar_list = []
    for bar in bars(api, symbol, trade_days, freq):
        length += len(bar.index)
        bar_list.insert(0, bar)
        if length >= max_length:
            break
    data = pd.concat(bar_list, ignore_index=True)
    return data


FREQ_MAP = {
    "M1": ("1M", lambda t: t-timedelta(minutes=1)), 
    "M5": ("5M", lambda t: t-timedelta(minutes=5)), 
    "M15": ("15M", lambda t: t-timedelta(minutes=15))
}

FREQ_REDIRECT = {
    "M3": ("M1", lambda t: t-timedelta(minutes=t.minute%3)),
    "M30": ("M15", lambda t: t-timedelta(minutes=t.minute%30)),
    "H1": ("M15", lambda t: t.replace(minute=0)),
}


def get_bar(api, symbol, trade_date, freq, **kwargs):
    try:
        freq, grouper= FREQ_MAP[freq]
    except KeyError:
        freq, grouper = FREQ_REDIRECT[freq]
        data = get_bar(api, symbol, trade_date, freq, **kwargs)
        return data.groupby(data["datetime"].apply(grouper)).agg(
            {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        ).reset_index()

    data, msg = api.bar(symbol, trade_date=trade_date, freq=freq, **kwargs)

    if msg == "0,":
        data["datetime"] = list(map(grouper, map(make_time, data["date"], data['time'])))
        return data[["datetime", "open", "high", "low", "close", "volume"]]
    else:
        raise Exception(msg)


def bars(api, symbol, trade_days, freq, retry=3):
    assert isinstance(api, DataApi)
    point = len(trade_days) -1
    while retry and (point >= 0):
        date = trade_days[point]
        try:
            yield get_bar(api, symbol, date, freq)
        except Exception as e:
            logging.error("get bar | %s | %s | %s | %s", symbol, date, freq, e)
            retry -= 1
        else:
            point -= 1


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


def get_api():
    api = DataApi(env.jaqs_addr)
    api.login(env.jaqs_user, env.jaqs_password)
    return api


def run():
    api = get_api()
    history(
        env.mongodb_uri, env.mongodb_db,
        env.listen_symbol, api, env.dates, env.listen_freq
    )


def main():
    import sys
    env.init(*sys.argv[1:], config="ctp.json", calendar="calendar.csv")
    run()


if __name__ == '__main__':
    main() 