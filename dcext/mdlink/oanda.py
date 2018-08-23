from dcext.proto.md_pb2 import MarketDataInd, MarketQuote, AskBid, QuoteStatic, MD_FUT_L1
from datetime import datetime, timedelta
from dcext import instruments
from dcext.oanda.stream import OandaStream
from dcext.zeromq import ZMQPublisher
from dcext.framework.transmit import Core
from dcext.proto.jzs_pb2 import MSG_MD_MARKETDATA_IND, Msg
import logging
import os


PUBSOCK = os.environ.get("OANDA_PUBSOCK", "tcp://*:10004")
TRADE_TYPE = os.environ.get("ONADA_TYPE", "PRACTICE")
TIMEZONE = int(os.environ.get("TIMEZONE", "8"))

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

    dt = datetime.strptime(price["time"][:26], FORMAT) + timedelta(hours=TIMEZONE)
    date = dt.year*10000+dt.month*100+dt.day
    millionsecond = (dt.hour*3600+dt.minute*60+dt.second)*1000+int(dt.microsecond/1000)
    
    qs.date = date
    qs.tradeday=date
    qs.preclose = candle["preclose"]
    
    quote.time = millionsecond
    jzcode = instruments.jzcode(price["instrument"])
    quote.jzcode = jzcode
    for name in PRICES:
        setattr(quote, name, candle[name])
    quote.turnover = sum([candle[name] for name in PRICES[:4]])/4*candle["volume"]
    quote.vwap = quote.turnover/quote.volume
    quote.last = quote.close
    return ind


def askbid(price, volume, pairs):
    for doc in pairs:
        price.append(float(doc[PRICE]))
        volume.append(doc[LIQUIDITY])
    fillab(price)
    fillab(volume)


def fillab(obj, fullfill=0, count=5):
    while len(obj) < 5:
        obj.append(fullfill)


def fill_zero(proto, names):
    for name in names:
        setattr(proto, name, 0)


def make_msg(ind):
    msg = Msg()
    msg.head.tid = MSG_MD_MARKETDATA_IND
    msg.head.src = "oanda"
    msg.head.dst = "all"
    msg.body = ind.SerializeToString()
    return msg


def int2bytes(buf, offset, value) :
    buf[offset+0] = value & 0xFF
    buf[offset+1] = (value>>8) & 0xFF
    buf[offset+2] = (value>>16) & 0xFF
    buf[offset+3] = (value>>24) & 0xFF


def msghead(value):
    buf = bytearray(4)
    int2bytes(buf, 0, value)
    return bytes(buf)


def msg_string(msg):
    return msghead(msg.head.tid) + msg.SerializeToString()


class OandaZMQCore(Core):

    def publish(self, content):
        try:
            ind = make_ind(content["price"], content["candle"])
            msg = make_msg(ind)
            message = msg_string(msg)
        except Exception as e:
            logging.error("parse msg to proto | %s | %s", content, e)
        else:
            logging.debug("parse msg to proto | %s | %s", content, msg.head)
            self.publisher.pub(message)


def run(addr, token, insts, sleep=10, trade_type="PRACTICE"):
    if isinstance(insts, str):
        insts = insts.split(",")
    publisher = ZMQPublisher.from_addr(addr)
    stream = OandaStream.conf(token, insts, sleep=sleep, trade_type=trade_type)
    core = OandaZMQCore(stream, publisher)
    core.start()


def command():
    from dcext.oanda.api import TOKEN
    instruments.init()
    run(PUBSOCK, TOKEN, instruments.find(14), trade_type=TRADE_TYPE)

    
if __name__ == '__main__':
    command()