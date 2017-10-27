package elasticsearch_interop

import org.apache.spark.sql.types.{DataType => SparkDataType, _}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._




object ESExportQuestionsPOC2 extends App {
  val path = "resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localApp")
  sparkConf.setMaster("local")

  val sc = new SparkContext(sparkConf)

  sc.setLogLevel("INFO")
  for(key <- sc.getConf.getAll) {
    println(key._1)
    println(key._2)
  }
  val sqlContext: SQLContext = new SQLContext(sc)

  //com.databricks.spark.csv.DefaultSource15 org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
  val df = sqlContext.read
    .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
    .option("header", "true").load("file:///Users/user/temp/f4m/ex1.csv");

  val df_schema: StructType = df.schema
  val namesCollection: Seq[String] = df_schema.map(p => p.name)

  /*val docsCollection: Array[Map[String, Any]] = df.head(2).map {
    case p: Row =>
      val vals: Seq[Any] = Range(0, p.length).map(index => p.get(index))
      (namesCollection zip vals).toMap
  }*/

  println("=========================== Starting saving to ES=======================")

  df.rdd.map {
    case p: Row =>
      val vals: Seq[Any] = Range(0, p.length).map(index => p.get(index))
      (namesCollection zip vals).toMap
  }.saveToEs("metrics_index/qdoc")


  //sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

  println("=========================== SAVING DONE          =======================")

  println("Done")

}