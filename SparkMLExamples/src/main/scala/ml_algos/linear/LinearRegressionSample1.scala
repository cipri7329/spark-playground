package ml_algos.linear

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame
import utils.{ResourcesUtil, SparkJob}


object LinearRegressionSample1 extends SparkJob {

  def main(args: Array[String]): Unit = {
    // Fit the model
    val lrModel: LinearRegressionModel = trainModel()

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val df = sampleDataset()
    df.show()

    val result = lrModel.transform(df)
    result.show()

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

  }

  def sampleDataset(): DataFrame = {
    val path = ResourcesUtil.resourcePath("/sample_linear_regression_data.txt")

    // Load training data
    val training = spark.read.format("libsvm")
      .load(path)

    training
  }

  def trainModel(): LinearRegressionModel = {
    // Load training data
    val training = sampleDataset()

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)
    lrModel
  }

  def getPipelineModel(): PipelineModel = {
    // Load training data
    val training = sampleDataset()

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val pipeline = new Pipeline()
      .setStages(Array(lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)
    model
  }

}
