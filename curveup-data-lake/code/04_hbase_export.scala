// 04_hbase_export.scala  -  cleaned zone -> TSV for HBase ImportTsv
// Row key = thread_id | 13-digit epoch millis | message_id
//   -> all messages of one conversation sit next to each other, in time order (rows sort by key)
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.session.timeZone", "UTC")   // dates and hours in UTC, whatever the VM clock says
spark.conf.set("spark.sql.shuffle.partitions", "4")     // small data: 4 tasks instead of 200, so we do not write hundreds of tiny files
val CLEAN = sys.env.getOrElse("CLEAN", "/datalake/clean/curveup/messages")
val OUT   = sys.env.getOrElse("HBASE_TSV", "/datalake/curated/hbase_tsv")

val m = spark.read.parquet(CLEAN)
val tsv = m.select(
    concat_ws("|", col("thread_id"),
      lpad((col("ts_utc").cast("double") * 1000).cast("long").cast("string"), 13, "0"),
      col("message_id").cast("string")).as("rowkey"),
    col("program_id"), col("channel"),                                  // META family
    col("role"), col("sender_type"), col("source"),                     // MSG family
    date_format(col("ts_utc"), "yyyy-MM-dd HH:mm:ss").as("ts_utc"),
    col("intent"), col("detected_language"),
    coalesce(col("confidence").cast("string"), lit("")).as("confidence"),
    col("content_flat"))
  .na.fill("")

// one tab-separated line per message, written as plain text (no CSV quoting)
tsv.select(concat_ws("\t", tsv.columns.map(col): _*).as("line"))
   .coalesce(1).write.mode("overwrite").text(OUT)
println(s"rows exported: ${spark.read.text(OUT).count()}")   // 1870
spark.read.text(OUT).show(3, 120)
