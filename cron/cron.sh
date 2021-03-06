#! /bin/bash

echo 'Asia/Shanghai' >/etc/timezone 
cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

ctime=(08:01 08:02 08:03)
ptime=(08:30 09:00 09:30)

echo Start OANDA M1 data collector.

echo "Time to create index set at ${ctime[*]}"
echo "Time to download data set at ${ptime[*]}"

echo "[CREATE INDEX] start at `date +%Y-%m-%dT%H:%M:%S`"
python dcext/mdlink/oanda_m1.py -n etc/oanda_m1.json create
echo "[CREATE INDEX] accomplish at `date +%Y-%m-%dT%H:%M:%S`"

echo "[DOANLOAD DATA] start at `date +%Y-%m-%dT%H:%M:%S`"
python dcext/mdlink/oanda_m1.py -n etc/oanda_m1.json publish
echo "[DOANLOAD DATA] accomplish at `date +%Y-%m-%dT%H:%M:%S`"

echo "[ENSURE INDEX] start at `date +%Y-%m-%dT%H:%M:%S`"
python dcext/mdlink/oanda_m1.py -n etc/oanda_m1.json ensure
echo "[ENSURE INDEX] accomplish at `date +%Y-%m-%dT%H:%M:%S`"



while true
do
    now=`date +%H:%M` 
    for t in ${ctime[*]} 
    do
        if [ $now == $t ]
        then
            echo "[CREATE INDEX] start at `date +%Y-%m-%dT%H:%M:%S`"
            python dcext/mdlink/oanda_m1.py -n etc/oanda_m1.json create
            echo "[CREATE INDEX] accomplish at `date +%Y-%m-%dT%H:%M:%S`"
            break
        fi

    done

    for p in ${ptime[*]} 
    do
        if [ $now == $p ]
        then
            echo "[DOANLOAD DATA] start at `date +%Y-%m-%dT%H:%M:%S`"
            python dcext/mdlink/oanda_m1.py -n etc/oanda_m1.json publish
            echo "[DOANLOAD DATA] accomplish at `date +%Y-%m-%dT%H:%M:%S`"
            break
        fi

    done
    
    if [ `date +%M` == "00" ]
    then
        echo routing output per hour at `date +%Y-%m-%dT%H:%M:%S`
    fi

    sleep 60
done