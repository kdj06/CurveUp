# CurveUp Data Lake

**ISIT312 Big Data Management · Autumn 2026 · University of Wollongong in Dubai**
Industry partner: **CurveUp** (Dubai) · Lecturer: Dr. Patrick Mukala

A data lake for CurveUp's chatbot conversations, built on the Hadoop ecosystem. It ingests the raw chat export into HDFS, cleans it with Spark, models it as a Hive data warehouse, serves single conversations from HBase, and processes new messages with Spark Structured Streaming. We then timed MapReduce, Hive, Spark and HBase on the same tasks to show which tool fits which job.

> **Data is not included.** The conversation data belongs to CurveUp and was shared with us anonymised for this project only. See [Data](#data).

---

## Architecture

```
 CurveUp export ──► Raw zone (HDFS) ──► Clean zone (Spark) ──► Curated zone ──┬──► Hive warehouse (OLAP)
  (JSONL)                ▲                                    (star schema)   └──► HBase (conversation lookup)
                         │
             Spark Structured Streaming (new messages, live counts, alerts)
```

HDFS is organised under `/datalake` into five zones, so each stage has its own location and never overwrites its source:

| Zone | Purpose |
|---|---|
| `landing` | New files arrive here first (used by the streaming demo) |
| `raw` | Unmodified source data, partitioned by ingest date; also holds the streaming output |
| `clean` | Validated messages as Parquet, partitioned by programme |
| `curated` | Star schema tables for Hive, and the TSV export for HBase |
| `rejects` | Rows that fail validation, kept for inspection |

![Architecture](docs/diagrams/architecture.png)

---

## Repository structure

```
curveup-data-lake/
├── README.md
├── .gitignore
├── code/        # all scripts, numbered in run order (00–06)
├── results/     # OLAP query output and timing logs
├── docs/
│   ├── diagrams/      # architecture, conceptual (MultiDim) and star schema
│   ├── screenshots/   # evidence from each step
│   ├── ISIT312_Report.pdf
│   └── ISIT312_Slides.pdf
└── demo/        # link to the screen recording of the streaming demo
```

---

## Environment

Everything runs on the course **BigDataVM** (single node):

| Component | Version |
|---|---|
| Hadoop (HDFS, YARN, MapReduce) | 2.7.3 |
| Spark | 3.0.3 (local mode) |
| Hive | 2.1.1 (on MapReduce) |
| HBase | [add version] |
| Zeppelin | for notebooks and charts |

The VM does not have enough memory to run Spark, Hive and HBase at the same time. Stop one before starting the next.

---

## How to run

Run the steps in order. Each script expects the previous step's output.

| Step | File(s) | Where to run | What it does |
|---|---|---|---|
| 0 | `00_start_services.sh` | Terminal | Starts HDFS, YARN and Zeppelin |
| 1 | `01_ingest.sh` | Terminal | Creates the five zones and loads the raw file into HDFS |
| 2 | `02_clean.scala` | `spark-shell` (`:load`) | Parses, validates and de-duplicates messages; writes Parquet to the clean zone |
| 3 | `03_curate.scala` → `03_hive.hql` → `03_olap.sql` | `spark-shell`, then Beeline (`!run`) | Builds the star schema, creates Hive tables, runs the 7 OLAP queries |
| 4 | `04_hbase_export.scala` → `04_hbase.hb` | `spark-shell`, then `hbase shell` | Exports a TSV, bulk-loads it with ImportTsv, queries by row key (`04_make_puts.py` is a backup loader if ImportTsv fails) |
| 5 | `05_split_by_day.py`, `05_stream.scala`, `05_replay.sh` | Terminal 1: `spark-shell`; Terminal 2: replay | Replays the history day by day through Spark Structured Streaming |
| 6 | `06_mr_mapper.py`, `06_mr_reducer.py`, `06_scale_up.scala` | Hadoop Streaming, `spark-shell` | Timing tests E1–E6 |

Place the export at `~/curveup/CurveUp_Anonymised_Conversations_2026-11-09/messages.jsonl` before step 1.

---

## Results

### Pipeline
- **1,872** raw messages (~726 KB) → **1,870** clean messages. Two test rows were dropped, and no timestamps failed to parse.
- Messages by sender: user 919 · bot 899 · staff 52.
- Star schema: `fact_message` 1,870 rows · `fact_conversation` 292 rows · `dim_date` 104 days.
- HBase: all 1,870 rows loaded with 0 bad lines.
- Streaming replay: 49 daily files, about 94 micro-batches, **1,870 messages landed** (nothing lost).

### Data warehouse
A **fact constellation**: `fact_message` (one row per message) and `fact_conversation` (one row per conversation) share the `dim_date`, `dim_program` and `dim_channel` dimensions, plus `dim_sender`, `dim_intent` and `dim_language`. `fact_message` is partitioned by month, and a copy is bucketed by `thread_id` (4 buckets, ORC).

### OLAP findings for CurveUp
| Question | Technique | Finding |
|---|---|---|
| Message volume | ROLLUP | MSME is 77% of traffic (1,438 messages) |
| Bot off-topic rate | CUBE | 27.5% overall; RUSH worst at 44.4% |
| Staff involvement | Conversation fact | Staff stepped in on 7.5% of MSME conversations |
| Reply speed | Percentile / AVG | Bots 0–5.3 s median; staff 31 s median but ~20 min mean |
| Busiest hours | RANK() | MSME peaks at 13:00 UTC |
| Week-on-week | LAG() | Traffic comes in bursts tied to programme events |
| Low confidence | GROUPING SETS | 177 replies below 0.6 confidence, 164 of them FAQs |

### Evaluation (median of 3 runs)
| Test | Task | Result |
|---|---|---|
| E1 | Count messages per chatbot | Spark 1.2 s · Hive 17.0 s · MapReduce 20.5 s |
| E2 | Open one conversation (8 messages) | HBase 0.079 s · Hive 0.240 s · Spark 0.430 s |
| E3 | Find all 52 staff replies (no row key) | HBase 0.184 s · Hive 0.232 s |
| E4 | E1 at 1,000× scale (1.87M messages, 743 MB) | Spark 3.2 s · Hive 26.6 s · MapReduce 37.1 s |
| E5 | Time until a new message is visible | Streaming 11.0 s · MapReduce rerun 18.4 s |
| E6 | Streaming output | 386 KB written as 49 files / 49 HDFS blocks (small-files problem) |

**Takeaway:** no single tool wins everything. Spark is fastest for bulk processing, HBase for opening one conversation by key, Hive for SQL reporting, and Structured Streaming for new data. MapReduce suits only large one-off batch jobs.

---

## Limitations
- One export only (1,870 messages); scale was tested with synthetic copies (×10 to ×1,000).
- Single-node VM, so timings don't reflect a real cluster.
- Hive on MapReduce takes ~30 s even for small queries, mostly job start-up.
- Streaming was simulated by replaying files, and it creates many small files. A scheduled compaction job would be needed in production.

---

## Data

The input is an anonymised export of CurveUp chatbot conversations (28 Jul – 8 Nov 2026), shared with the team for this course project only. It is **not** in this repository, and `.gitignore` blocks data files from being committed. To run the code, place your own copy of `messages.jsonl` at the path above.

---

## Team

| Name | Role |
|---|---|
| Khadejeh | Pipeline (ingest and cleaning) |
| Marina | Pipeline (ingest and cleaning) |
| Shahd | Warehouse (schema design, Hive and OLAP) |
| Amani | Warehouse (schema design, Hive and OLAP) |
| Riyad | Serving (HBase and Streaming) |
| Malak | Serving (HBase and Streaming) |
| Diya | Evaluation (experiments E1–E6) |
| Sushma | Evaluation (experiments E1–E6) |
| Anasha | Report |
| Ilham | Report |

Thanks to CurveUp for sharing their data and feedback.
