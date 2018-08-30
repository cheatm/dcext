import json
import pandas as pd


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


def load(config_file, inst_file="", date_file=""):
    with open(config_file) as f:
        conf = json.load(f)
    
    for name in ["jaqs", "mongodb", "tick", "jqbar"]:
        replace(name, conf.get(name, {}))
    
    listen = conf.get("listen", {})
    listen_symbol.update(set(listen.get("symbol", [])))
    listen_freq.update(set(listen.get("freq", [])))

    if inst_file:
        insts = pd.read_csv(inst_file).set_index("symbol")
        for symbol  in listen_symbol:
            mapper[insts.loc[symbol, "jzcode"]] = symbol 

    if date_file:
        globals()["dates"] = pd.read_csv(date_file)["date"]


def replace(head, conf):
    for key, value in conf.items():
        name = "%s_%s" % (head, key)
        globals()[name] = value


def get_table_name(name, gran):
    mapped = mongodb_map.get(name, name)
    return "%s_%s" % (mapped, gran)


def main():
    load(r"D:\DataServer\etc\ctp.json", r"D:\DataServer\etc\instrument.csv")
    print(get_table_name("rb1901.SHF", "M1"))




if __name__ == '__main__':
    main()