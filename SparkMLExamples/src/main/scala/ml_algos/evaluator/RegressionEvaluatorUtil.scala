package ml_algos.evaluator

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame

object RegressionEvaluatorUtil {

  def RMSE(dataset: DataFrame, labelCol: String = "label", predictionCol: String = "prediction"): Double = {
    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(dataset)
    rmse
  }


}
