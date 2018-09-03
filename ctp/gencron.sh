echo "export CTP_HOME=$PWD" > /etc/profile.d/ctp_env.sh
echo "50 8,12,20 * * 1-5 /bin/bash $PWD/ctpdocker.sh start >> $PWD/cronlog 2>&1
10 12,15,23 * * 1-5 /bin/bash $PWD/ctpdocker.sh stop >> $PWD/cronlog 2>&1" > schedule