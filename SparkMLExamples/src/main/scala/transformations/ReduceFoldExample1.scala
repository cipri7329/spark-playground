package transformations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReduceFoldExample1 extends App{

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localReduceFoldExample1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val rdd1 = spark.sparkContext.parallelize(List[Int](3))

  val sum = rdd1.reduce{ (x, y) => x + y}

  val sum2 = rdd1.fold(10){ (x, y) => x + y}
  println(sum)
  println(sum2)


}


