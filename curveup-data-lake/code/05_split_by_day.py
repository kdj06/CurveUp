# 05_split_by_day.py  -  split messages.jsonl into one file per day, to replay as a live stream
# usage: python3 05_split_by_day.py messages.jsonl ~/curveup/replay
import json, os, sys
src, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
files = {}
with open(src, encoding="utf-8") as f:
    for line in f:
        day = json.loads(line)["created_at"][:10]
        if day not in files:
            files[day] = open(os.path.join(out, "msgs_%s.jsonl" % day), "w", encoding="utf-8")
        files[day].write(line)
for h in files.values():
    h.close()
print("%d daily files written to %s" % (len(files), out))
