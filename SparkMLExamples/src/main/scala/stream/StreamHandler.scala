package stream

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

import scala.collection.immutable.HashMap


trait StreamHandler {

  implicit val spark: SparkSession

  implicit val streamHandler: StreamHandler = new StreamHandler

  class StreamHandler {
    private var streamQueryList: HashMap[String, StreamingQuery] = HashMap.empty

    def createInputKafkaStream(host: String, port: Int, topic: String): DataFrame = {
      val df: DataFrame = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", s"$host:$port")
        .option("subscribe", topic)
        .load()
      df
    }

    def createOutputKafkaStream(host: String, port: Int, topic: String): DataFrame = ???

    def createInputSocketStream(host: String, port: Int, includeTimestamp: Boolean = false): DataFrame = {
      val df: DataFrame = spark
        .readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .option("includeTimestamp", includeTimestamp)
        .load()
      df
    }

    def createOutputSocketStream(host: String, port: Int, topic: String): DataFrame = ???

    def createConsoleOutputStream(name: String, stream: DataFrame, outputMode: String = "complete", truncate: Boolean = false): StreamingQuery = {
      val query: StreamingQuery = stream.writeStream
        .outputMode(outputMode)
        .format("console")
        .option("truncate", truncate)
        .start()

      addStreamQuery(name, query)
      query
    }

    def addStreamQuery(name: String, query: StreamingQuery): StreamingQuery = {
      streamQueryList = streamQueryList + (name -> query)
      query
    }

    def awaitAllStreamQueries(): Unit = {
      streamQueryList.foreach(query => query._2.awaitTermination())
    }

    def awaitStreamQuery(queryName: String): Unit = {
      streamQueryList.get(queryName).foreach(query => query.awaitTermination())
    }
  }
}
