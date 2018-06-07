package stream

import org.apache.spark.sql.DataFrame
import utils.SparkJob
import org.apache.spark.sql.functions._

/**
  * start the console producer:
  *   kafka-console-producer --broker-list 127.0.0.1:9092 --topic first_topic
  *
  *   enter text by typing new lines
  */

object SparkKafkaExample1 extends SparkJob{

  def main(args: Array[String]) {

//    if (args.length < 2) {
//      System.err.println("Usage: SparkKafkaExample1 <hostname> <port>")
//      System.exit(1)
//    }

    import spark.implicits._

    val host = "localhost" //args(0)
    val port = 9092 // args(1).toInt
    val topic = "first_topic"

    System.out.println(s"Usage: SparkKafkaExample1 <hostname> <port> <topic>: $host $port $topic")

    // Subscribe to 1 topic
    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$host:$port")
      .option("subscribe", topic)
      .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    // Subscribe to multiple topics
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//      .option("subscribe", "topic1,topic2")
//      .load()
//    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]

    // Subscribe to a pattern
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//      .option("subscribePattern", "topic.*")
//      .load()
//    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]

    println(s"is streaming: ${df.isStreaming}")

    val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val df2 = df1.select("value").flatMap( row => row.getString(0).split(" ")).groupBy("value").agg(count(col("*").as("total")))

    df.createOrReplaceTempView("updates")
    val df3 = spark.sql("select count(*) as rows from updates")  // returns another streaming DF

    // Start running the query that prints the running counts to the console
    // prints the last line
    val query1 = df1.writeStream
      .outputMode("append")
      .format("console")
      .start()

    // prints the unique words and number of occurence
    val query2 = df2.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    //prints the total number of lines read so far
    val query3 = df3.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
  }



}
