#!/usr/bin/env python3
# 06_mr_mapper.py - Hadoop Streaming mapper: (program_id, 1) for every message
# Python 3.4-safe (the VM runs Ubuntu 14.04): no f-strings
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        rec = json.loads(line)
    except ValueError:
        continue                       # skip broken lines instead of failing the job
    prog = rec.get("program_id")
    if prog:
        print("%s\t1" % prog)
