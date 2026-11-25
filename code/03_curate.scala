// 03_curate.scala  -  cleaned zone -> curated star schema (Parquet files that Hive reads)
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

spark.conf.set("spark.sql.session.timeZone", "UTC")   // dates and hours in UTC, whatever the VM clock says
spark.conf.set("spark.sql.shuffle.partitions", "4")     // small data: 4 tasks instead of 200, so we do not write hundreds of tiny files
val CLEAN = sys.env.getOrElse("CLEAN", "/datalake/clean/curveup/messages")
val DW    = sys.env.getOrElse("DW",    "/datalake/curated/curveup_dw")
val m = spark.read.parquet(CLEAN)

// surrogate key = row number over the natural key (Rule 1, L5-6)
def withKey(df: org.apache.spark.sql.DataFrame, key: String, orderCol: String) =
  df.withColumn(key, row_number().over(Window.orderBy(orderCol)))

// ---------- dimensions ----------
// full calendar from first to last day, so days and weeks with no messages still appear
val (d0, nDays) = m.agg(min("msg_date"), datediff(max("msg_date"), min("msg_date")))
                   .as[(java.sql.Date, Int)].first()
val dimDate = spark.range(0, nDays + 1).toDF("offset")
  .withColumn("full_date", expr(s"date_add(to_date('$d0'), cast(offset as int))")).drop("offset")
  .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
  .withColumn("day_name", date_format(col("full_date"), "EEEE"))
  .withColumn("week_of_year", weekofyear(col("full_date")))
  .withColumn("month", date_format(col("full_date"), "yyyy-MM"))
  .withColumn("year", year(col("full_date")))

val dimProgram = withKey(m.select("program_id").distinct(), "program_key", "program_id")
  .withColumn("main_project", col("program_id").isin("msme", "orla", "rush"))

val dimChannel = withKey(m.select("channel").distinct(), "channel_key", "channel")

val dimSender = withKey(m.select("role", "sender_type", "source").distinct()
  .withColumn("sk", concat_ws("|", col("role"), col("source"))), "sender_key", "sk").drop("sk")

val dimIntent = withKey(m.select("intent").distinct(), "intent_key", "intent")
  .withColumn("intent_group",
     when(col("intent").isin("faq", "programmes", "booking", "enrolled", "account"), "answered")
    .when(col("intent").isin("greeting", "acknowledge"), "small talk")
    .when(col("intent") === "handoff", "handoff")
    .when(col("intent") === "off_topic", "off topic")
    .otherwise("not labelled"))

val dimLanguage = withKey(m.select("detected_language").distinct(), "language_key", "detected_language")
  .withColumn("language_group",
     when(col("detected_language") === "english", "English")
    .when(col("detected_language").isin("tamil", "sinhala", "nepali", "arabic"), "Local language")
    .when(col("detected_language").isin("singlish", "tanglish", "arabizi"), "Code-mixed (romanised)")
    .otherwise("Unknown"))

// ---------- fact 1: one row per message ----------
val factMessage = m
  .join(dimProgram.select("program_id", "program_key"), "program_id")
  .join(dimChannel, "channel")
  .join(dimSender.select("role", "source", "sender_key"), Seq("role", "source"))
  .join(dimIntent.select("intent", "intent_key"), "intent")
  .join(dimLanguage.select("detected_language", "language_key"), "detected_language")
  .withColumn("date_key", date_format(col("msg_date"), "yyyyMMdd").cast("int"))
  .select(col("message_id"), col("thread_id"),               // degenerate dimensions
          col("date_key"), col("msg_hour_utc"), col("program_key"), col("channel_key"),
          col("sender_key"), col("intent_key"), col("language_key"),
          lit(1).as("message_count"), col("content_length"), col("confidence"),
          col("response_time_sec"), col("msg_month"))

// ---------- fact 2: one row per conversation (thread) ----------
val factConversation = m.groupBy("thread_id", "program_id", "channel").agg(
    min("ts_utc").as("first_ts"), max("ts_utc").as("last_ts"),
    count(lit(1)).as("n_messages"),
    sum(when(col("sender_type") === "user", 1).otherwise(0)).as("n_user_msgs"),
    sum(when(col("sender_type") === "bot", 1).otherwise(0)).as("n_bot_msgs"),
    sum(when(col("sender_type") === "staff", 1).otherwise(0)).as("n_staff_msgs"),
    sum(when(col("intent") === "off_topic", 1).otherwise(0)).as("n_off_topic"),
    avg("response_time_sec").as("avg_response_sec"))
  .withColumn("duration_min", (col("last_ts").cast("double") - col("first_ts").cast("double")) / 60)
  .withColumn("had_staff_reply", (col("n_staff_msgs") > 0).cast("int"))
  .withColumn("start_date_key", date_format(col("first_ts"), "yyyyMMdd").cast("int"))
  .join(dimProgram.select("program_id", "program_key"), "program_id")
  .join(dimChannel, "channel")
  .drop("program_id", "channel", "first_ts", "last_ts")

// ---------- write ----------
dimDate.write.mode("overwrite").parquet(s"$DW/dim_date")
dimProgram.write.mode("overwrite").parquet(s"$DW/dim_program")
dimChannel.write.mode("overwrite").parquet(s"$DW/dim_channel")
dimSender.write.mode("overwrite").parquet(s"$DW/dim_sender")
dimIntent.write.mode("overwrite").parquet(s"$DW/dim_intent")
dimLanguage.write.mode("overwrite").parquet(s"$DW/dim_language")
factMessage.write.mode("overwrite").partitionBy("msg_month").parquet(s"$DW/fact_message")
factConversation.write.mode("overwrite").parquet(s"$DW/fact_conversation")

println(s"fact_message rows: ${spark.read.parquet(s"$DW/fact_message").count()}")          // 1870
println(s"dim_date rows: ${spark.read.parquet(s"$DW/dim_date").count()}")
println(s"fact_conversation rows: ${spark.read.parquet(s"$DW/fact_conversation").count()}") // 292

Seq("dim_date","dim_program","dim_sender","fact_message","fact_conversation").foreach{t => println(t); spark.read.parquet(s"$DW/$t").printSchema()}
