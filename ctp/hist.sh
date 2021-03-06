#! /bin/bash

echo 'Asia/Shanghai' >/etc/timezone 
cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

check=(08:50 10:20 10:25 11:35 13:20 15:30 20:30 23:10)

echo "Time to check set at ${check[*]}"

while true
do
    now=`date +%H:%M` 
    for t in ${check[*]} 
    do
        if [ $now == $t ]
        then 
            echo "[check and reload] start at `date +%Y-%m-%dT%H:%M:%S`"
            python dcext/mm/ctp/hist.py
            echo "[check and reload] accomplish at `date +%Y-%m-%dT%H:%M:%S`"
            break
        fi
    done

    if [ `date +%M` == "00" ]
    then
        echo hist check alive `date +%Y-%m-%dT%H:%M:%S`
    fi

    sleep 60
done