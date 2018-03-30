package utils

import org.apache.spark.sql.functions._

object UdfUtils {

  @inline val arrayAvg = udf( (column : Seq[Double]) =>
    {
      val validData = column.filter(_ > 0)
      val sum = validData.sum
      sum / validData.length.toDouble
    }
  )



}
