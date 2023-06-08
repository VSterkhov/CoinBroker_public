#!/bin/bash
sudo service mysql start

echo "The cluster will be started"
ip_address=$(hostname -I | tr -d ' ')

echo "
UPDATE hcatalog.SERDE_PARAMS
SET PARAM_VALUE = REPLACE (PARAM_VALUE, '$LAST_IP', '$ip_address');

UPDATE hcatalog.DBS
SET DB_LOCATION_URI = REPLACE (DB_LOCATION_URI, '$LAST_IP', '$ip_address');

UPDATE hcatalog.CTLGS
SET LOCATION_URI = REPLACE (LOCATION_URI, '$LAST_IP', '$ip_address');

UPDATE hcatalog.SDS
SET LOCATION = REPLACE (LOCATION, '$LAST_IP', '$ip_address');
" > update_ip_hcatalog.sql

echo "Update hcatalog script created..."

echo "Enter the MySQL root password..."
mysql -u root -p < update_ip_hcatalog.sql

echo "Mysql hive meta updated."

fileList=(
	"/usr/local/hadoop/etc/hadoop/hdfs-site.xml"
	"/usr/local/hadoop/etc/hadoop/core-site.xml"
	"/usr/local/hadoop/etc/hadoop/mapred-site.xml"
	"/usr/local/hadoop/etc/hadoop/yarn-site.xml"
	"/usr/local/spark/conf/spark-defaults.conf"
	"/home/toor/hue/desktop/conf/pseudo-distributed.ini"
)
echo "Change ip addresses in configurations"
for path in "${fileList[@]}"; do   # The quotes are necessary here
	sed -i "s/${LAST_IP}/${ip_address}/g" $path
	echo "Fixed - ${path}"
done
echo "The last ip - ${LAST_IP} changed to ${ip_address} succsessfuly"

sed -i "s/${LAST_IP}/${ip_address}/g" "${HOME}/.bashrc"
echo "Fixed ip in ${HOME}/.bashrc"

sudo rm /run/nologin
sudo service sshd restart

sudo systemctl start mysql.service

bash start-all.sh
bash start-history-server.sh

#hue runserver > /home/toor/logs/hue_server.log 2>&1 &

hive --service hiveserver2 > /home/toor/logs/hiveserver2.log 2>&1 &

source /home/toor/airenv/bin/activate
airflow webserver > /home/toor/logs/airflow_webserver.log 2>&1 &
airflow scheduler > /home/toor/logs/airflow_scheduler.log 2>&1 &
