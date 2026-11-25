// 06_scale_up.scala  -  SYNTHETIC copies of the export (x10, x100, x1000) for the scalability test only.
// Each copy gets new message_ids and thread_ids, so counts and joins still behave. Label results as synthetic.
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.session.timeZone", "UTC")
val RAW = sys.env.getOrElse("RAW", "/datalake/raw/curveup/messages/ingest_date=2026-11-09")
val OUT = sys.env.getOrElse("SCALE_OUT", "/datalake/bench")
val base = spark.read.json(RAW)
val n = base.count()
for (factor <- Seq(10, 100, 1000)) {
  val copies = spark.range(factor).toDF("copy")
  val scaled = base.crossJoin(copies)
    .withColumn("message_id", col("message_id") + col("copy") * n)
    .withColumn("thread_id", concat(col("thread_id"), lit("_c"), col("copy")))
    .drop("copy")
  scaled.write.mode("overwrite").json(s"$OUT/x$factor")
  println(s"x$factor rows: ${spark.read.json(s"$OUT/x$factor").count()}")
}
