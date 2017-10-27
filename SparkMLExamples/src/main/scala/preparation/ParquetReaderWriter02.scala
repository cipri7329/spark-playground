package preparation

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object ParquetReaderWriter02 extends App {
  val path = "resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localApp")
  sparkConf.setMaster("local")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("INFO")
  val sqlContext: SQLContext = new SQLContext(sc)

  import sqlContext.implicits._

  println(sqlContext.sparkSession.conf)
  println(sqlContext.sparkSession.catalog.listDatabases())
  println(sqlContext.sparkSession.catalog.listTables())

  val df = sqlContext.read.parquet("file:///Users/user/work/POC/spark-ml-training/scala/SparkMLExamples/spark-warehouse/table2")
  println(df.columns)

  df.collect.foreach(println)



  println("Done")

}