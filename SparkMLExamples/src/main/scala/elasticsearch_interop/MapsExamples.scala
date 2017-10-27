package elasticsearch_interop

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._


object MapsExamples extends App{
  val path = "resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localApp")
  sparkConf.setMaster("local")
  sparkConf.set("es.index.auto.create", "true");

  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("INFO")
  val sqlContext: SQLContext = new SQLContext(sc)

  println(sqlContext.sparkSession.conf)
  println(sqlContext.sparkSession.catalog.listDatabases())
  println(sqlContext.sparkSession.catalog.listTables())

  val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
  val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

  Seq(numbers,airports).foreach(println)
  val es_rdd: RDD[Map[String, Any]] = sc.makeRDD(Seq(numbers, airports))
  es_rdd.saveToEs("spark/docs")
  val myMap: Map[Any, AnyVal] = Map(1->2,"test"->4.5)

}
