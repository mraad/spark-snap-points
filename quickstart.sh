#!/usr/bin/env bash

DAEMONS="\
cloudera-quickstart-init \
zookeeper-server \
hadoop-hdfs-datanode \
hadoop-hdfs-namenode \
hadoop-hdfs-secondarynamenode \
hadoop-httpfs \
hadoop-yarn-nodemanager \
hadoop-yarn-resourcemanager \
spark-history-server"

for daemon in ${DAEMONS}; do
    sudo service ${daemon} start
done
