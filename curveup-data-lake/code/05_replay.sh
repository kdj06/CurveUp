#!/bin/bash
# 05_replay.sh  -  drop one day of messages into HDFS every few seconds (simulates WhatsApp/web traffic)
# usage: bash 05_replay.sh ~/curveup/replay 5
DIR=$1; PAUSE=${2:-5}
for f in $(ls $DIR/msgs_*.jsonl | sort); do
  $HADOOP_HOME/bin/hadoop fs -put $f /datalake/landing/stream/_tmp_$(basename $f)
  $HADOOP_HOME/bin/hadoop fs -mv /datalake/landing/stream/_tmp_$(basename $f) /datalake/landing/stream/$(basename $f)
  echo "$(date +%T) sent $(basename $f) ($(wc -l < $f) messages)"
  sleep $PAUSE
done
