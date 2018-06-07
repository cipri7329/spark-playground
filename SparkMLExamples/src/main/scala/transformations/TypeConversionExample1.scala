package transformations

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.DataframeSchemaUtil

object TypeConversionExample1 extends App{

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localKMeans1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  import org.apache.spark.sql.functions._

  val s1 = Seq(
    (1,"a", "d", 0L, false, 1.2, Array("a","b","c"), Array(Array("a")), "a".getBytes),
    (2,"b", "e", 0L, false, 1.2, Array("a","b","c"), Array(Array("a")),"ba".getBytes),
    (1,"a", "g", 0L, false, 1.2, Array("a","b","c"), Array(Array("a")),"acdef".getBytes),
    (2,"b", "d", 0L, false, 1.2, Array("a","b","c"), Array(Array("a")),"aefdsqer".getBytes)
  )

  import spark.implicits._

  val d1 = s1.toDF("id", "fname", "lname", "isbn", "employed", "gpa", "string_arry", "array2", "code")

  d1.show(false)
  d1.printSchema()

  val jsonSchema = DataframeSchemaUtil.serializeSchema(d1)
  println(jsonSchema)

  val deserializedSchema = DataframeSchemaUtil.deserializeSchema(jsonSchema)

  /*
  {"type":"struct",
   "fields":[
      {"name":"id","type":"integer","nullable":false,"metadata":{}},
      {"name":"fname","type":"string","nullable":true,"metadata":{}},
      {"name":"lname","type":"string","nullable":true,"metadata":{}},
      {"name":"isbn","type":"long","nullable":false,"metadata":{}},
      {"name":"employed","type":"boolean","nullable":false,"metadata":{}},
      {"name":"gpa","type":"double","nullable":false,"metadata":{}},
      {"name":"code","type":"binary","nullable":true,"metadata":{}}]
  }
   */

  println(deserializedSchema)

  //group by and count by all columns
//  d1.groupBy(d1.columns.map(col(_)):_*).count.show

  var resultDF = d1

  def convertAllToString(df: DataFrame): DataFrame = {
    var resultDF = df
    val newType = "string"
    for(name:String <- df.columns){
      resultDF = convertColumn(resultDF, name, newType)
    }
    resultDF
  }

  def convertColumn(df: DataFrame, name:String, newType:String): DataFrame = {
    val old_column_tag = "_old_column_name"
    val df_1 = df.withColumnRenamed(name, old_column_tag)
    df_1.withColumn(name, df_1.col(old_column_tag).cast(newType)).drop(old_column_tag)
  }

  resultDF = convertAllToString(resultDF)

  resultDF.show(false)
  resultDF.printSchema()


}
