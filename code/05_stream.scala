// 05_stream.scala  -  Spark Structured Streaming over the landing folder (L12)
// Start this first, then run 05_replay.sh in a second Terminal.
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

spark.conf.set("spark.sql.session.timeZone", "UTC")   // dates and hours in UTC, whatever the VM clock says
val LANDING = sys.env.getOrElse("LANDING", "/datalake/landing/stream")
val LAKE    = sys.env.getOrElse("STREAM_OUT", "/datalake/raw/curveup/messages_stream")
val CKPT    = sys.env.getOrElse("CKPT", "/datalake/_checkpoints")
spark.conf.set("spark.sql.shuffle.partitions", "3")

val schema = StructType(Seq(
  StructField("message_id", LongType), StructField("thread_id", StringType),
  StructField("program_id", StringType), StructField("channel", StringType),
  StructField("role", StringType), StructField("source", StringType),
  StructField("created_at", StringType), StructField("content", StringType),
  StructField("intent", StringType), StructField("detected_language", StringType),
  StructField("confidence", DoubleType)))

val events = spark.readStream.schema(schema)
  .option("maxFilesPerTrigger", 1)                 // one "day" per micro-batch
  .json(LANDING)                                   // files starting with _ are ignored
  .filter(col("program_id").isNotNull)
  .withColumn("ts_utc", col("created_at").cast("timestamp"))

// Query 1 - incremental ETL: every new message lands in the lake as Parquet (append mode)
val toLake = events.writeStream.format("parquet")
  .option("path", LAKE).option("checkpointLocation", s"$CKPT/to_lake")
  .outputMode("append").start()

// Query 2 - live dashboard: messages per chatbot per day (event-time window + watermark)
val perDay = events.withWatermark("ts_utc", "2 days")
  .groupBy(window(col("ts_utc"), "1 day"), col("program_id")).count()
val dash = perDay.writeStream.format("console").option("truncate", "false")
  .outputMode("update").trigger(Trigger.ProcessingTime("5 seconds"))
  .option("checkpointLocation", s"$CKPT/dash").start()

// Query 3 - alert: the bot did not understand the user, or staff had to reply
val alerts = events
  .filter(col("intent") === "off_topic" || col("source") === "admin")
  .select(col("ts_utc"), col("program_id"), col("thread_id"), col("intent"), col("source"))
val alertQ = alerts.writeStream.format("console").outputMode("append")
  .option("checkpointLocation", s"$CKPT/alerts").start()

// In spark-shell / Zeppelin leave them running; stop with: spark.streams.active.foreach(_.stop())
