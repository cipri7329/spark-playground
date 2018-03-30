package transformations

import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row}
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, TextInputCSVDataSource}
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.functions._
import utils.SparkJob
import utils.TimeUtil.TimeProfilerUtility._

object DataFrameReaderApp extends App with SparkJob{

  override def main(args: Array[String]): Unit = {

//    time(DataFrameReaderApp.readLargeFileWithoutSchema())
    println("====warm up finished====")

//    time(DataFrameReaderApp.readLargeFileWithSchema())
//    time(DataFrameReaderApp.readLargeFileWithSchemaLimited())
//    time(DataFrameReaderApp.readLargeFileWithoutSchema())
    time(DataFrameReaderApp.readLargeFileWithoutSchemaLimited())
//    time(DataFrameReaderApp.readLargeFileWithSchemaOptimized())
    time(DataFrameReaderApp.readLargeFileWithSchemaOptimizedLimited())

  }

  def userHomeDirectory: String = System.getProperty("user.home")

  lazy val filePath = userHomeDirectory + "/tmp/genomics/fact_patients_sample.csv"
//  lazy val filePath = "s3a://file.engine.tests/load_s3/csv/fact_patients_sample.csv"

  lazy val LIMIT = 150

  def readLargeFileWithSchema(): DataFrame = {
    println("===read WITH schema - no limit===")

    val dataFrameReader: DataFrameReader = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("inferSchema", true)

    val df = dataFrameReader.csv(filePath)

    df.first()
    df.count()
    df.show()
    df.printSchema()
    df
  }

  def readLargeFileWithSchemaLimited(): Dataset[Row] = {
    println("===read WITH schema - with limit===")

    val dataFrameReader: DataFrameReader = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("inferSchema", true)

    val df = dataFrameReader.csv(filePath).limit(LIMIT)

    df.first()
    df.count()
    //    df.show()
    //    df.printSchema()
    df
  }

  def readLargeFileWithoutSchema(): DataFrame = {
    println("===read WITHOUT schema - no limit===")

    val dataFrameReader: DataFrameReader = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("inferSchema", false)

    val df = dataFrameReader.csv(filePath)

    df.first()
    df.count()
//    df.show()
//    df.printSchema()
    df
  }

  def readLargeFileWithoutSchemaLimited(): DataFrame = {
    println("===read WITHOUT schema - with limit===")
    val sparkSession = spark


    val dataFrameReader: DataFrameReader = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("inferSchema", false)

    val df = dataFrameReader.csv(filePath)
      .head(LIMIT)
//      .limit(LIMIT)

    println(df.length)
//    println(df2.first())
//    println(df2.count())
//    df2.show()
    //    df.printSchema()
    null
  }

  def readLargeFileWithSchemaOptimized(): Dataset[Row] = {
    println("===read WITHOUT schema optimized - no limit===")
    val newSchema: StructType = inferSchema(filePath)

    val dataFrameReader: DataFrameReader = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(newSchema)

    var df: Dataset[Row] = dataFrameReader.csv(filePath)//.limit(LIMIT)
//    df.printSchema()
//    df.show()
//    println()
//    println(s"csv first line: ${df.rdd.first()}")

//    val newDF = spark.createDataFrame(df.rdd, newSchema)
//    var newDF = applyNewSchema(df, newSchema)
    var newDF = df
    println("=== NEW SCHEMA ===")
    newDF.printSchema()


//    println(newDF.first())
    newDF.first()
    newDF.show()
    newDF.count()
//    println()
//    println(s"total elements: ${df.count()}")
//    df.printSchema()
    newDF
  }

  def readLargeFileWithSchemaOptimizedLimited(): Dataset[Row] = {
    println("===read WITHOUT schema opimized - with limit===")
    val newSchema: StructType = inferSchema(filePath)
    newSchema.printTreeString()

    val dataFrameReader: DataFrameReader = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(newSchema)

    var df: Dataset[Row] = dataFrameReader.csv(filePath)//.limit(LIMIT)
    //    df.printSchema()
    //    df.show()
    //    println()
    //    println(s"csv first line: ${df.rdd.first()}")

    //    val newDF = spark.createDataFrame(df.rdd, newSchema)
    //    var newDF = applyNewSchema(df, newSchema)
    var newDF = df
    //    println("=== NEW SCHEMA ===")
    //    newDF.printSchema()


    //    println(newDF.first())
    newDF.first()
    //    newDF.show()
    newDF.count()
    //    println()
    //    println(s"total elements: ${df.count()}")
    //    df.printSchema()
    newDF
  }

  def applyNewSchema(dataset: Dataset[Row], schema:StructType): Dataset[Row] = {
    var ds = dataset
    for(field <- schema.fields) {
      val fieldName = field.name
      val fieldType = field.dataType
      ds = ds.withColumn(fieldName, col(fieldName).cast(fieldType))
    }
    ds
  }

  def inferSchema(file: String): StructType = {
    val dataFrameReader: DataFrameReader = spark.read
    val dataSample: Array[String] = dataFrameReader.textFile(file).head(LIMIT)
    val firstLine = dataSample.head

    import spark.implicits._
    val ds: Dataset[String] = spark.createDataset(dataSample)

    val extraOptions = new scala.collection.mutable.HashMap[String, String]
    extraOptions += ("inferSchema" -> "true")
    extraOptions += ("header" -> "true")
    extraOptions += ("delimiter" -> ",")

    val csvOptions: CSVOptions = new CSVOptions(extraOptions.toMap, spark.sessionState.conf.sessionLocalTimeZone)
    val schema: StructType = TextInputCSVDataSource.inferFromDataset(spark, ds, Some(firstLine), csvOptions)

    schema
  }

}
