package preparation

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col

object ParquetReaderWriter011 extends App {
  val path = "resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localApp")
  sparkConf.setMaster("local")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("INFO")
  val sqlContext: SQLContext = new SQLContext(sc)

  import sqlContext.implicits._


  val df = Seq((1, 2, 3 ,4, 5, 6)).toDF("col1","col2","col3","col4","col5","col6")

  val colNos = Seq(0, 3, 5)

  val res = df.columns.map(col)
  println(res)

  val res2 = colNos map df.columns map col
  println(res2)
  df.select(colNos map df.columns map col: _*).explain


  println("Done")

}