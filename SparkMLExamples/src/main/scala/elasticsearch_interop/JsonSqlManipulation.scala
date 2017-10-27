package elasticsearch_interop

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object JsonSqlManipulation extends App {
  val path = "resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localApp")
  sparkConf.setMaster("local")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("INFO")
  val sqlContext: SQLContext = new SQLContext(sc)

  val eventsDF = sqlContext.jsonFile("file:///Users/user/temp/f4m_questions/player_game_end.json")
  eventsDF.createOrReplaceTempView("stats_player_game_end")
  //create a database
  /*
    sqlContext.sql("select * from stats_player_game_end limit 10")
    val questionUsage = sqlContext.sql(
      """
          SELECT
              eventTimestamp,
              to_date(from_unixtime(eventTimestamp/1000, 'YYYY-MM-dd')) as eventDate,
              gameId,
              eq.questionId,
              eq.answerCorrect,
              eq.answerSpeed
          FROM stats_player_game_end
          LATERAL VIEW EXPLODE(questions) exp_q as eq
      """)
      .createOrReplaceTempView("stats_question_usage");*/

  sqlContext.sql(
    """
        SELECT
            eventTimestamp,
            to_date(from_unixtime(eventTimestamp/1000, 'YYYY-MM-dd')) as eventDate,
            gameId,
            eq.questionId,
            eq.answerCorrect,
            eq.answerSpeed
        FROM stats_player_game_end
        LATERAL VIEW EXPLODE(questions) exp_q as eq
    """).repartition(1)
        .write
        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat") //com.databricks.spark.csv.DefaultSource15 org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
        .option("header", "true")
        .save("file:///Users/user/temp/f4m_questions/stats_question_usage_temp2.csv")
}