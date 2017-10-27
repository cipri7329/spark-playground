package preparation

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object ParquetReaderWriter01 extends App {
  val path = "resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localApp")
  sparkConf.setMaster("local")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("INFO")
  val sqlContext: SQLContext = new SQLContext(sc)

  import sqlContext.implicits._


  val list = List(1, 2, 3)


  val rdd = sc.parallelize(list).map(x => (x, x * x, x + x))
  val df = rdd.toDF("__col1", "col.c22", "__col3")
  df.createOrReplaceTempView("gogu")

  sqlContext.sql("select 1,2 from gogu").collect.foreach(println)

  println("Done")

}