package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  */
trait SparkJob {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  implicit lazy val spark:SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName(this.getClass.getSimpleName)
//    .enableHiveSupport()
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()


  def readCsv(path: String, schema: StructType): DataFrame = {
    println(s"===###: reading csv with schema from: $path")
    spark.read.schema(schema).csv(path)
  }

  /**
    * read csv with header
    * @param path
    * @return
    */
  def readCsv(path:String): DataFrame = {
    println(s"===###: reading csv from: $path")
    spark.read.option("header", "true").csv(path)
  }

  def readJson(path: String, schema: StructType): DataFrame = {
    println(s"===###: reading json from: $path")
    spark.read.schema(schema).json(path)
  }

  def readParquet(path:String): DataFrame = {
    println(s"===###: reading parquet from: $path")
    spark.read.parquet(path)
  }


  def saveAsParquet[T](dataset: Dataset[T], path: String, partitionByColumn: Option[String]): Unit = {
    println(s"===###: saving as parquet to: $path")
    var writer = dataset.write
    writer = writer.mode("overwrite").format("parquet")
    if (partitionByColumn.isDefined)
      writer = writer.partitionBy(partitionByColumn.get)
    writer.save(path)
  }

  def saveAsTable[T](dataset: Dataset[T], path: String, partitionByColumn: Option[String]): Unit = {
    println(s"===###: saving as hive table to: $path")
    var writer = dataset.write
    writer = writer.mode("overwrite")
    if (partitionByColumn.isDefined)
      writer = writer.partitionBy(partitionByColumn.get)
    writer.saveAsTable(path)
  }

  def saveAsCSV[T](dataset: Dataset[T], path: String, partitionByColumn: Option[String]): Unit = {
    println(s"===###: saving as csv to: $path")
    var writer = dataset.write
    writer = writer.mode("overwrite").option("header", "true").format("csv")
    if (partitionByColumn.isDefined)
      writer = writer.partitionBy(partitionByColumn.get)
    writer.save(path)
  }

  def sparkJob: SparkJob = this
}
