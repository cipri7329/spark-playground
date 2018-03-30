package utils

import org.apache.spark.sql.DataFrame

/**
  * import this object with
  * import utils.DataFrameUtils._
  * and use the methods as implicit methods applied on a dataframe
  */
object DataFrameUtils {

  implicit class DataFrameTransforms(dataFrame: DataFrame) {

    def prefixColumns(prefix: String): DataFrame = {
      var df = dataFrame
      for(col <- dataFrame.columns)
        df = df.withColumnRenamed(col, prefix+col)
      df
    }

  }


}