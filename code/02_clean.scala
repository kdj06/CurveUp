// 02_clean.scala  -  raw zone (JSONL) -> cleaned zone (Parquet)
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

spark.conf.set("spark.sql.session.timeZone", "UTC")   // dates and hours in UTC, whatever the VM clock says
spark.conf.set("spark.sql.shuffle.partitions", "4")     // small data: 4 tasks instead of 200, so we do not write hundreds of tiny files
val RAW   = sys.env.getOrElse("RAW",   "/datalake/raw/curveup/messages/ingest_date=2026-11-09")
val CLEAN = sys.env.getOrElse("CLEAN", "/datalake/clean/curveup/messages")

// 1. Read with an explicit schema (L11: always define the schema in production)
val schema = StructType(Seq(
  StructField("message_id", LongType),        StructField("thread_id", StringType),
  StructField("program_id", StringType),      StructField("channel", StringType),
  StructField("role", StringType),            StructField("source", StringType),
  StructField("created_at", StringType),      StructField("content", StringType),
  StructField("intent", StringType),          StructField("detected_language", StringType),
  StructField("confidence", DoubleType)))
val raw = spark.read.schema(schema).json(RAW)
println(s"raw rows: ${raw.count()}")

// 2. Clean
val cleaned = raw
  .filter(col("program_id").isNotNull)                    // drops the 'upsert test' thread
  .dropDuplicates("message_id")
  .withColumn("ts_utc", col("created_at").cast("timestamp"))
  .withColumn("msg_date", to_date(col("ts_utc")))
  .withColumn("msg_month", date_format(col("ts_utc"), "yyyy-MM"))
  .withColumn("msg_hour_utc", hour(col("ts_utc")))
  .withColumn("sender_type",
     when(col("role") === "user", "user")
    .when(col("source") === "admin", "staff")
    .otherwise("bot"))
  .withColumn("detected_language", coalesce(col("detected_language"), lit("unknown")))
  .withColumn("intent", coalesce(col("intent"), lit("none")))
  .withColumn("content_length", length(col("content")))
  .withColumn("content_flat", regexp_replace(col("content"), "[\\t\\r\\n]+", " "))

// Quality gate: every timestamp must parse; failed rows go to the rejects zone
val badTs = cleaned.filter(col("ts_utc").isNull)
println(s"rows with unparseable timestamps: ${badTs.count()}")
badTs.write.mode("overwrite").json(sys.env.getOrElse("REJECTS", "/datalake/rejects/curveup/messages_bad_ts"))

// 3. Response time: seconds between a user message and the reply that follows it
val w = Window.partitionBy("thread_id").orderBy("ts_utc", "message_id")
val withRt = cleaned
  .filter(col("ts_utc").isNotNull)
  .withColumn("prev_role", lag("role", 1).over(w))
  .withColumn("prev_ts",   lag("ts_utc", 1).over(w))
  .withColumn("response_time_sec",
     when(col("role") === "assistant" && col("prev_role") === "user",
          col("ts_utc").cast("double") - col("prev_ts").cast("double")))
  .drop("prev_role", "prev_ts", "created_at")

// 4. Write Parquet, partitioned by programme and month
withRt.write.mode("overwrite").partitionBy("program_id", "msg_month").parquet(CLEAN)

val check = spark.read.parquet(CLEAN)
println(s"clean rows: ${check.count()}")          // expect 1870
check.groupBy("program_id").count().orderBy(desc("count")).show()
check.groupBy("sender_type").count().show()     // user 919, bot 899, staff 52

// IF THE QUALITY GATE REPORTS UNPARSEABLE TIMESTAMPS (older Spark on the VM):
// replace the ts_utc line in step 2 with this one (all times in the export are UTC):
//   .withColumn("ts_utc", to_timestamp(substring(col("created_at"), 1, 19), "yyyy-MM-dd'T'HH:mm:ss"))
