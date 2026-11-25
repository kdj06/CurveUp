#!/usr/bin/env python3
# 06_mr_reducer.py - Hadoop Streaming reducer: sum the counts per program_id (input arrives sorted by key)
import sys
current, total = None, 0
for line in sys.stdin:
    key, val = line.rstrip("\n").split("\t", 1)
    if key != current:
        if current is not None:
            print("%s\t%d" % (current, total))
        current, total = key, 0
    total += int(val)
if current is not None:
    print("%s\t%d" % (current, total))
