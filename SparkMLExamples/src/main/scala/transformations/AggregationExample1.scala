package transformations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.UdfUtils

object AggregationExample1 extends App{

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localKMeans1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val s1 = Seq((1,"a", "d", 4, 4),(2,"b", "e", 4, 3),(0,"a", "g", 5, 1),(2, "b", "d", 0, 1))

  var d1 = spark.createDataFrame(s1)

  d1.groupBy("_1","_2").count.show

  d1.createOrReplaceTempView("temp1")

  //group by and count by all columns
  d1.groupBy(d1.columns.map(col(_)):_*).count.show

  d1.show()

  val predictionCols = List("_1", "_4", "_5")
  d1 = d1.withColumn("power", predictionCols.map(col).foldRight(lit(0)) {
      (c1, c2) => {
        val p1 = when(c1.isNaN, c2).otherwise(c1) //defaults to other value
        val p2 = when(c2.isNaN, lit(0)).otherwise(c2) //defaults to zero
        val avg1 = p1+p2 //defaults to zero
        avg1 //average value. discard average with zero
      }
    }
  )

  d1 = d1.withColumn("power2", array(predictionCols.map(col) :_*))
  d1 = d1.withColumn("power2", UdfUtils.arrayAvg(col("power2")))

  /*
    +---+---+---+---+---+-----+------+
    | _1| _2| _3| _4| _5|power|power2|
    +---+---+---+---+---+-----+------+
    |  1|  a|  d|  4|  4|    9|   3.0|
    |  2|  b|  e|  4|  3|    9|   3.0|
    |  0|  a|  g|  5|  1|    6|   3.0|
    |  2|  b|  d|  0|  1|    3|   1.5|
    +---+---+---+---+---+-----+------+
   */
  d1.show()


  /*
  +---+-----------+
  | _1|avg(power2)|
  +---+-----------+
  |  1|        3.0|
  |  2|       2.25|
  |  0|        3.0|
  +---+-----------+
   */
  d1.groupBy("_1").agg(avg("power2").as("power2")).show()
  //sum columns
  //var totalPower = power.withColumn("sum", stationIds.map(col).reduce((c1, c2) => c1 + c2))
}
