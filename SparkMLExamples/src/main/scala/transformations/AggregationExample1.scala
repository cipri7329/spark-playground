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


  val s1 = Seq((1,"a"),(2,"b"),(1,"a"),(2, "b"))

  val d1 = spark.createDataFrame(s1)

  d1.groupBy("_1","_2").count.show



}
