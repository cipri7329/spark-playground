package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  */
trait SparkJob {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  implicit lazy val spark:SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName(this.getClass.getSimpleName)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()


  def readCsv(path: String, schema: StructType): DataFrame = spark.read.schema(schema).csv(path)

  def readJson(path: String, schema: StructType): DataFrame = spark.read.schema(schema).json(path)

  def sparkJob = this
}
