package transformations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AggregationExample1 extends App{

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localKMeans1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  import org.apache.spark.sql.functions._

  val s1 = Seq((1,"a", "d"),(2,"b", "e"),(1,"a", "g"),(2, "b", "d"))

  val d1 = spark.createDataFrame(s1)

  d1.groupBy("_1","_2").count.show

  d1.createOrReplaceTempView("temp1")

  //group by and count by all columns
  d1.groupBy(d1.columns.map(col(_)):_*).count.show
}
