package ml_algos

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object KMeansTitanic1 extends App {
  val path = "src/main/resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localKMeans2App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  // Load and parse the data file, converting it to a DataFrame.
//  val data = spark.read.format("libsvm").load(path+"sample_libsvm_data.txt")
//  println (data.first)


 /*
     k-means is an unsupervised iterative algorithm that groups input data in a predefined number of k clusters.
     Each cluster has a centroid which is a cluster center.
     It is a highly iterative machine learning algorithm that
     measures the distance (between a vector and centroids) as the nearest mean.
     The algorithm steps are repeated till the convergence of a specified number of steps.
  */

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
  val imputedTrainMap = Map[String, Any]("Age" -> avgAge, "Embarked" -> "S", "Fare" -> avgFare)

  // Dropping rows containing any null values.
  val titanicDataset = rawTitanicDataset.na.fill(imputedTrainMap)

  titanicDataset.printSchema()
  titanicDataset.show(truncate = false)

  // Split the data into training and test sets (30% held out for testing).
  val Array(trainingData, testData) = titanicDataset.randomSplit(Array(0.7, 0.3))



  val stringCols = Seq("Sex", "Embarked")
  val indexers = stringCols.map { colName =>
    new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "Indexed")
  }

  val numericCols = Seq("Age", "SibSp", "Parch", "Fare", "Pclass")

    /*
    --columns Pclass,Sex,Age,SibSp,Parch,Fare,Embarked
    */

  val featuresCol = "features"
  val assembler = new VectorAssembler()
    .setInputCols((numericCols ++ stringCols.map(_ + "Indexed")).toArray)
    .setOutputCol(featuresCol)


//  val labelCol = "Survived"
//  val decisionTree = new DecisionTreeClassifier()
//    .setLabelCol(labelCol)
//    .setFeaturesCol(featuresCol)


  // Trains a k-means model.
  val kmeans = new KMeans()
    .setFeaturesCol(featuresCol)
    .setK(2)
    .setMaxIter(1000)

  println(kmeans.explainParams)

  /*
    featuresCol: features column name (default: features, current: features)
    initMode: The initialization algorithm. Supported options: 'random' and 'k-means||'. (default: k-means||)
    initSteps: The number of steps for k-means|| initialization mode. Must be > 0. (default: 2)
    k: The number of clusters to create. Must be > 1. (default: 2, current: 2)
    maxIter: maximum number of iterations (>= 0) (default: 20, current: 1000)
    predictionCol: prediction column name (default: prediction)
    seed: random seed (default: -1689246527)
    tol: the convergence tolerance for iterative algorithms (>= 0) (default: 1.0E-4)
   */

//  / initialize pipeline stages
  val stages = (indexers :+ assembler :+ kmeans).toArray

  val pipeline = new Pipeline()
    .setStages(stages)

  val model = pipeline.fit(trainingData)

  val predictions = model.transform(testData)



  predictions.show()

  predictions.sample(false, 0.5).show()

  /*  --columns Pclass,Sex,Age,SibSp,Parch,Fare,Embarked
      --clusters 2
      -n kmodel
      --maxIterations 1000",
  */

  val kmeansModel = model.stages(stages.length-1).asInstanceOf[KMeansModel]
  val centers = kmeansModel.clusterCenters
  // Shows the result.
  println("Cluster Centers: ")
  centers.foreach(println)
  println("Cluster Centers2: ")
  kmeansModel.clusterCenters.map(_.toSparse).foreach(println)

  // Evaluate clustering by computing Within Set Sum of Squared Errors.
  val WSSSE = kmeansModel.computeCost(testData)
  println(s"Within Set Sum of Squared Errors = $WSSSE")



}