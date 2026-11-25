-- 03_hive.hql  -  Hive tables over the curated Parquet files (run in Beeline: !run 03_hive.hql, or %hive)
-- Needs: Hive metastore + hiveserver2 running (Lab 4), and 03_curate.scala already run.

CREATE DATABASE IF NOT EXISTS curveup_dw LOCATION '/datalake/curated/curveup_dw/_hive';
USE curveup_dw;

-- Dimensions (external: dropping a table never deletes the data)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
  full_date DATE, date_key INT, day_name STRING, week_of_year INT, month STRING, year INT)
STORED AS PARQUET LOCATION '/datalake/curated/curveup_dw/dim_date';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_program (
  program_id STRING, program_key INT, main_project BOOLEAN)
STORED AS PARQUET LOCATION '/datalake/curated/curveup_dw/dim_program';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_channel (
  channel STRING, channel_key INT)
STORED AS PARQUET LOCATION '/datalake/curated/curveup_dw/dim_channel';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_sender (
  role STRING, sender_type STRING, source STRING, sender_key INT)
STORED AS PARQUET LOCATION '/datalake/curated/curveup_dw/dim_sender';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_intent (
  intent STRING, intent_key INT, intent_group STRING)
STORED AS PARQUET LOCATION '/datalake/curated/curveup_dw/dim_intent';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_language (
  detected_language STRING, language_key INT, language_group STRING)
STORED AS PARQUET LOCATION '/datalake/curated/curveup_dw/dim_language';

-- Fact 1: one row per message, partitioned by month (range partitioning on time, L7)
CREATE EXTERNAL TABLE IF NOT EXISTS fact_message (
  message_id BIGINT, thread_id STRING, date_key INT, msg_hour_utc INT,
  program_key INT, channel_key INT, sender_key INT, intent_key INT, language_key INT,
  message_count INT, content_length INT, confidence DOUBLE, response_time_sec DOUBLE)
PARTITIONED BY (msg_month STRING)
STORED AS PARQUET LOCATION '/datalake/curated/curveup_dw/fact_message';
MSCK REPAIR TABLE fact_message;   -- registers the msg_month=... folders Spark wrote
SHOW PARTITIONS fact_message;

-- Fact 2: one row per conversation (constellation schema: shares dim_program and dim_channel)
CREATE EXTERNAL TABLE IF NOT EXISTS fact_conversation (
  thread_id STRING, n_messages BIGINT, n_user_msgs BIGINT, n_bot_msgs BIGINT,
  n_staff_msgs BIGINT, n_off_topic BIGINT, avg_response_sec DOUBLE, duration_min DOUBLE,
  had_staff_reply INT, start_date_key INT, program_key INT, channel_key INT)
STORED AS PARQUET LOCATION '/datalake/curated/curveup_dw/fact_conversation';

-- Checks: expect 1870, 292 and 104
SELECT COUNT(*) FROM fact_message;
SELECT COUNT(*) FROM fact_conversation;
SELECT COUNT(*) FROM dim_date;

-- Physical design evidence (Lab 5, B2/B5): partition pruning and bucketing
EXPLAIN EXTENDED SELECT COUNT(*) FROM fact_message WHERE msg_month = '2026-09';

SET hive.enforce.bucketing = true;
CREATE TABLE IF NOT EXISTS fact_message_bkt (
  message_id BIGINT, thread_id STRING, date_key INT, msg_hour_utc INT,
  program_key INT, channel_key INT, sender_key INT, intent_key INT, language_key INT,
  message_count INT, content_length INT, confidence DOUBLE, response_time_sec DOUBLE,
  msg_month STRING)
CLUSTERED BY (thread_id) INTO 4 BUCKETS STORED AS ORC;
INSERT OVERWRITE TABLE fact_message_bkt SELECT * FROM fact_message;
EXPLAIN EXTENDED SELECT * FROM fact_message_bkt WHERE thread_id = 't_3369d6bcd9c2fc97';
