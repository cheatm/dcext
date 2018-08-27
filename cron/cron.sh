

ctime=(14:21 14:22 14:23)
ptime=(08:30 09:00 09:30)

echo Start OANDA M1 data collector.

echo "Time to create index set at ${ctime[*]}"
echo "Time to download data set at ${ptime[*]}"


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