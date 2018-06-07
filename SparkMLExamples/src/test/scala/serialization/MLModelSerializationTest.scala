package serialization
import java.nio.charset.StandardCharsets

import ml_algos.evaluator.RegressionEvaluatorUtil
import ml_algos.linear.LinearRegressionSample1
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.DataFrame
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import utils.TestSparkJob
import utils.serialization.{JsonConfiguration, JsonUtil, Marshal}

class MLModelSerializationTest
  extends SpecificationWithJUnit with BeforeAfterAll with TestSparkJob {

  def beforeAll: Unit = {

    val pipelineModel: PipelineModel = LinearRegressionSample1.getPipelineModel()
    val lrModel = pipelineModel.stages(0).asInstanceOf[LinearRegressionModel]

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    model = pipelineModel

    sampleData = LinearRegressionSample1.sampleDataset()
  }

  def afterAll: Unit = println("afterAll")

  private var model: PipelineModel = _
  private var sampleData: DataFrame = _

  "Java serialization" should {

    "marshal model" in {

      val serializedModel: Array[Byte] = Marshal.dump(model)
      val deserializedModel: PipelineModel = Marshal.load[PipelineModel](serializedModel)

      val result = deserializedModel.transform(sampleData)
      result.show()

      val rmse = RegressionEvaluatorUtil.RMSE(result)
      println(s"rmse: $rmse")

      rmse must be < 11.0
    }

  }



}
