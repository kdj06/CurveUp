#!/bin/bash
# 00_start_services.sh  -  start Hadoop + Zeppelin on the BigDataVM (run every session, in a Terminal)
$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
$ZEPPELIN_HOME/bin/zeppelin-daemon.sh start
jps
# Hive (Phase 3), in two extra Terminals:  $HIVE_HOME/bin/hive --service metastore   and   $HIVE_HOME/bin/hiveserver2
# HBase (Phase 4):                          $HBASE_HOME/bin/start-hbase.sh
