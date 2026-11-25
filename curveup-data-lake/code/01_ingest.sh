#!/bin/bash
# 01_ingest.sh  -  create the data lake folders and load the CurveUp export into the raw zone
H=$HADOOP_HOME/bin/hadoop
SRC=${1:-~/curveup/CurveUp_Anonymised_Conversations_2026-11-09}
D=ingest_date=2026-11-09

$H fs -mkdir -p /datalake/raw/curveup/messages/$D /datalake/raw/curveup/threads/$D /datalake/raw/curveup/meta/$D
$H fs -mkdir -p /datalake/clean /datalake/curated /datalake/landing/stream /datalake/rejects

$H fs -put -f $SRC/messages.jsonl /datalake/raw/curveup/messages/$D/
$H fs -put -f $SRC/threads.csv    /datalake/raw/curveup/threads/$D/
$H fs -put -f $SRC/summary.json $SRC/README.md /datalake/raw/curveup/meta/$D/

$H fs -ls -R /datalake/raw
echo "lines in raw messages (expect 1872):"
$H fs -cat /datalake/raw/curveup/messages/$D/messages.jsonl | wc -l
$H fs -stat "block size %o, replication %r" /datalake/raw/curveup/messages/$D/messages.jsonl
