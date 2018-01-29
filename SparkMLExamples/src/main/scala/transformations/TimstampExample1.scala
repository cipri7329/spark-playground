package transformations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TimstampExample1 extends App{

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localKMeans1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  import org.apache.spark.sql.functions._

  val s1 = Seq(
    (1,"a", "d", "2017-01-01 12:23:14"),
    (2,"b", "e", "2017-01-01 12:23:14"),
    (3,"a", "g", "2017-01-01 12:23:14"),
    (4, "b", "d", "1970-01-01 00:00:00")
  )

  import spark.implicits._

  val d1 = s1.toDF("id", "fname", "lname", "date")

  var t1 = d1.withColumn("timestamp1", to_utc_timestamp($"date", "GMT"))
  t1 = t1.withColumn("timestamp2", unix_timestamp($"timestamp1", "UTC"))

  t1.printSchema()
  t1.show(false)

  t1.createOrReplaceTempView("tmp")

  val t3 = spark.sql("select *, unix_timestamp(timestamp1, 'UTC') * 1000 as timestamp3 from tmp")

  spark.sql(" SELECT *,\n unix_timestamp(timestamp1, 'UTC') * 1000 as timestamp\n FROM tmp")
  t3.printSchema()
  t3.show(false)
}
