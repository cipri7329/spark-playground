package stream

import org.apache.spark.sql.{DataFrame, Dataset}
import utils.SparkJob
import org.apache.spark.sql.functions._
import java.sql.Timestamp

import org.apache.spark.sql.streaming.StreamingQuery
/**
  * example taken from:
  *   https://github.com/apache/spark/blob/v2.3.0/examples/src/main/scala/org/apache/spark/examples/sql/streaming/StructuredNetworkWordCount.scala
  *
  * Counts words in UTF8 encoded, '\n' delimited text received from the network.
  *
  * Usage: StructuredNetworkWordCount <hostname> <port>
  * <hostname> and <port> describe the TCP server that Structured Streaming
  * would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  * and then run the example
  *    `$ bin/run-example sql.streaming.StructuredNetworkWordCount
  *    localhost 9999`
  */

object StructuredNetworkWordCountExample extends SparkJob{

  def main(args: Array[String]) {

//    if (args.length < 2) {
//      System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }

    val host = "localhost" //args(0)
    val port = 9999 // args(1).toInt

    System.out.println(s"Usage: StructuredNetworkWordCount <hostname> <port>: $host $port")


    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    println(s"is streaming: ${lines.isStreaming}")
    lines.printSchema()

    // Split the lines into words
    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
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
    val query1 = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    /*
      https://stackoverflow.com/questions/45618489/executing-separate-streaming-queries-in-spark-structured-streaming
      Summary: Each query in Structured Streaming consumes the source data. The socket source creates a new connection for each query defined. The behavior seen in this case is because nc is only delivering the input data to the first connection.
      Henceforth, it's not possible to define multiple aggregations over the socket connection unless we can ensure that the connected socket source delivers the same data to each connection open.

      Try with Kafka
     */

    val query2: StreamingQuery = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query1.awaitTermination()
    query2.awaitTermination()
  }

}
