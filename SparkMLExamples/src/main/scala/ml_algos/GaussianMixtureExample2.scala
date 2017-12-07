package ml_algos

import ml_algos.RandomForest1.model
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DoubleType

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object GaussianMixtureExample2 extends App {
  val path = "src/main/resources/"

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localGaussianMixtureExample2App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  // Load and parse the data file, converting it to a DataFrame.
  //  val data = spark.read.format("libsvm").load(path+"sample_libsvm_data.txt")
  //  println (data.first)
  import spark.implicits._

  val trainFilePath = path+"titanic_formatted.csv"

  import spark.implicits._
  // Loads data.
  val rawTitanicDataset = spark
    .read
    //    .format("csv")
    .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(trainFilePath)
    .withColumn("Survived", $"Survived".cast(DoubleType))
    .cache()

  rawTitanicDataset.printSchema()
  rawTitanicDataset.show(5, truncate = false)
  rawTitanicDataset.describe("Age").show()
  rawTitanicDataset.describe("Fare").show()


  //clean dataset
  val avgAge = rawTitanicDataset.select(avg("Age")).first().getDouble(0)
  val avgFare = rawTitanicDataset.select(avg("Fare")).first().getDouble(0)
  //  val imputedTrainMap = Map[String, Any]("Age" -> avgAge, "Cabin" -> "", "Embarked" -> "S", "Ticket" -> "113803", "Sex" -> 1, "Fare" -> avgFare)
  val imputedTrainMap = Map[String, Any]("Age" -> avgAge, "Cabin" -> "", "Ticket" -> "", "Embarked" -> "S", "Fare" -> avgFare)

  // Dropping rows containing any null values.
  val titanicDataset = rawTitanicDataset.na.fill(imputedTrainMap)

  titanicDataset.printSchema()
  titanicDataset.show(truncate = false)

  // Split the data into training and test sets (30% held out for testing).
  val Array(trainingData, testData) = titanicDataset.randomSplit(Array(0.7, 0.3))

  val stringCols = Seq("Sex", "Embarked")
  //  val stringCols = Seq("Cabin")
  val indexers = stringCols.map { colName =>
    new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "Indexed")
      .setHandleInvalid("skip")
  }

  val numericCols = Seq("Age", "Fare")

  val featuresCol = "features"
  val assembler = new VectorAssembler()
    .setInputCols(numericCols.toArray)
    .setOutputCol(featuresCol)


  // Trains Gaussian Mixture Model
  val gmm = new GaussianMixture()
    .setFeaturesCol(featuresCol)
    .setK(3)


  val stages = (Seq() :+ assembler :+ gmm).toArray

  val pipeline = new Pipeline()
    .setStages(stages)

  val model = pipeline.fit(trainingData)

  val predictions = model.transform(testData)

  val probabilityToList = udf( (ubixId:Long, predictions: DenseVector) => {
    val membershipValues = predictions.toArray.zipWithIndex.sortBy(_._1)(Ordering[Double].reverse)
    val topMemberships = membershipValues.take(3)
      .toList.flatMap { case (value: Double, clusterId: Int) => List(clusterId, value) }
    topMemberships
  })

  predictions.show(false)

  val df_filtered = predictions.withColumn("ubix_result", probabilityToList(col("PassengerId"), col("probability")))

  df_filtered.show(false)

  val df_result = df_filtered.select("PassengerId", "ubix_result")

//  val rdd2 = df_result.rdd.map{ case (ubixId:Long, values:List[Any]) => (ubixId, values)}.rdd
//  println(rdd2.first())

  val gmmModel = model.stages(1).asInstanceOf[GaussianMixtureModel]

  // output parameters of mixture model model
  for (i <- 0 until gmmModel.getK) {
    println(s"Gaussian $i:\nweight=${gmmModel.weights(i)}\n" +
      s"mu=${gmmModel.gaussians(i).mean}\nsigma=\n${gmmModel.gaussians(i).cov}\n")
  }

}