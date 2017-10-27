package preparation

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


object HiveSqlManipulation extends App {
  val path = "resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localApp")
  sparkConf.setMaster("local")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("INFO")
  val sqlContext: SQLContext = new SQLContext(sc)

  import sqlContext.implicits._


  //create a database
  sqlContext.sql("create database test")
  sqlContext.sql("use test")
  println(sqlContext.sparkSession.catalog.currentDatabase)

  val list = List(1, 2, 3)

  val rdd = sc.parallelize(list).map(x => (x, x * x, x + x))
  val df = rdd.toDF("__col1", "col.c22", "__col3")
  println(df.columns)

  df.registerTempTable("gogu")

  //create a dataframe

  sqlContext.sql("select * from gogu").collect().foreach(println)
  println("Done")

}