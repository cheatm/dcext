#! /bin/bash

python dcext/mm/ctp/hist.py etc/ctp.json etc/calendar.csv
python dcext/mm/ctp/run.py etc/ctp.json etc/instrument.csv etc/market.csv