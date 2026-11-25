# 04_make_puts.py  -  BACKUP ONLY: use if ImportTsv fails.
# Turns the HBase TSV into HBase shell 'put' commands.
# usage:
#   hadoop fs -cat /datalake/curated/hbase_tsv/part-* > hbase.tsv
#   python3 04_make_puts.py hbase.tsv > puts.hb
#   $HBASE_HOME/bin/hbase shell < puts.hb      (takes a few minutes for 1,870 rows)
import sys

COLS = ["META:program_id", "META:channel", "MSG:role", "MSG:sender_type", "MSG:source",
        "MSG:ts_utc", "MSG:intent", "MSG:language", "MSG:confidence", "MSG:content"]

def q(s):
    # single-quoted Ruby string: only backslash and single quote need escaping
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

with open(sys.argv[1], encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 11:
            continue
        key, values = parts[0], parts[1:]
        for col, val in zip(COLS, values):
            if val != "":
                print("put 'CONVERSATION', %s, %s, %s" % (q(key), q(col), q(val)))
print("count 'CONVERSATION'")
print("exit")
