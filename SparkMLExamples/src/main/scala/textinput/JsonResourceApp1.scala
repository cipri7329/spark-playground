package textinput

import utils.{Resources, SparkJob}

import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonResourceApp1 extends App with SparkJob {


  val hbaseJson = "/json/hbase-extract-individual.json"

  val path = Resources.resourcePath(hbaseJson)

  println(path)
  val jsonLines = spark.read.json(path).collect()


  jsonLines.foreach(println)


}

