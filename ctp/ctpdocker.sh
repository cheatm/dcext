#! /bin/bash

source /etc/profile
cd $CTP_HOME

if [ $1 == "start" ]
then
    docker-compose up -d
else
    docker-compose stop
fi