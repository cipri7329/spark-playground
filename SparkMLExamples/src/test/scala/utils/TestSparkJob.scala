package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  */
trait TestSparkJob extends SparkJob{

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  override implicit lazy val spark:SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName("Test_" + this.getClass.getSimpleName)
//    .enableHiveSupport()
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

}
