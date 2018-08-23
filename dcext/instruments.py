import pandas as pd
import os


ETC = os.environ.get("ETC_PATH", ".")
# INSTRUMENT_FILE = os.environ.get("INSTRUMENT", "instrument.csv")
INSTRUMENT_FILE = os.path.join(ETC, "instrument.csv")
# MARKET_FILE = os.environ.get("MARKET", "market.csv")
MARKET_FILE = os.path.join(ETC, "market.csv")


class Mapper(object):

    def jzcode(self, symbol):
        pass
    
    def get_symbols(self, market):
        pass


mapper = Mapper()


class FileMapper(Mapper):

    def __init__(self, instrument_file):
        self.instrument_file = instrument_file
        self.instruments = pd.read_csv(instrument_file, index_col="instcode")
        self.jzcode_map = self.instruments["jzcode"].to_dict()

    def get_symbols(self, market):
        return list(self.instruments[self.instruments.market==market].index)

    def jzcode(self, symbol):
        return self.jzcode_map[symbol]


def init():
    globals()["mapper"] = FileMapper(INSTRUMENT_FILE) 


def jzcode(symbol):
    return mapper.jzcode(symbol)


def find(market):
    return mapper.get_symbols(market)


if __name__ == '__main__':
    init()
    # print(jzcode("AUD_JPY"))
    print(find(14))