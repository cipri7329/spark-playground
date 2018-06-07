package stream

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import utils.SparkJob

/**
  * start the console producer:
  *   kafka-console-producer --broker-list 127.0.0.1:9092 --topic first_topic
  *
  *   enter text by typing new lines
  */

object WindowedSparkKafkaExample1 extends SparkJob with StreamHandler {

  def main(args: Array[String]) {

//    if (args.length < 2) {
//      System.err.println("Usage: SparkKafkaExample1 <hostname> <port>")
//      System.exit(1)
//    }

    import spark.implicits._

    val host = "localhost" //args(0)
    val port = 9092 // args(1).toInt
    val topic = "first_topic"

    System.out.println(s"Usage: ${this.getClass.getName} <hostname> <port> <topic>: $host $port $topic")

    // Subscribe to 1 topic
    val df: DataFrame = streamHandler.createInputKafkaStream(host, port, topic)
    df.printSchema()

    val lines = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .as[(String, String, Timestamp)]


    // Split the lines into words
    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, String, Timestamp)].flatMap(line =>
      line._2.split(" ").map(word => (word, line._3))
    ).toDF("word", "timestamp")

    // Generate running word count
    val wordCounts: DataFrame = words.groupBy("word").count()

    val windowDuration = s"10 seconds"
    val slideDuration = s"5 seconds"
    val windowedCounts: DataFrame = words.groupBy(
      window($"timestamp", windowDuration, slideDuration),
      $"word"
    ).count().orderBy("window")

    // Start running the query that prints the running counts to the console
    val query1 = streamHandler.createConsoleOutputStream("wordCounts", wordCounts)
    /*
      https://stackoverflow.com/questions/45618489/executing-separate-streaming-queries-in-spark-structured-streaming
      Summary: Each query in Structured Streaming consumes the source data. The socket source creates a new connection for each query defined. The behavior seen in this case is because nc is only delivering the input data to the first connection.
      Henceforth, it's not possible to define multiple aggregations over the socket connection unless we can ensure that the connected socket source delivers the same data to each connection open.

      Try with Kafka
     */

    val query2: StreamingQuery = streamHandler.createConsoleOutputStream("windowedCounts", windowedCounts)

    streamHandler.awaitAllStreamQueries()

    //TODO: count unique characters read so far
    //TODO: how to kill a stream?
    //TODO: stream statistics (kafka or spark)
    //TODO: stream throttling
    //TODO: stream scheduled data generation
    //TODO: trigger event (action) when some condition is met
    //TODO: pipe a table to a series of commands equivalent with pipeing a stream through the series of commands
    //TODO: if stream batch reaches a certain size, only then process

  }



}
