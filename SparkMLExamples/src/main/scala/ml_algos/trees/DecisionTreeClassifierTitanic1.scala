package ml_algos.trees

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.DoubleType
import utils.SparkJob

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object DecisionTreeClassifierTitanic1 extends SparkJob {
  val path = "src/main/resources/"

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
//  val titanicTrain = spark
//    .read
//    .option("header", "true")
//    .option("inferSchema", "true")
//    .csv(trainFilePath)
//    .withColumn("Survived", $"Survived".cast(DoubleType))
//    .cache()

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
  val featuresCol = "features"
  val assembler = new VectorAssembler()
    .setInputCols((numericCols ++ stringCols.map(_ + "Indexed")).toArray)
    .setOutputCol(featuresCol)


  val labelCol = "Survived"
  val decisionTree = new DecisionTreeClassifier()
    .setLabelCol(labelCol)
    .setFeaturesCol(featuresCol)

  val pipeline = new Pipeline().setStages((indexers :+ assembler :+ decisionTree).toArray)

  val model = pipeline.fit(trainingData)


  val predictions = model.transform(testData)



  predictions.show()



  /*  --columns Pclass,Sex,Age,SibSp,Parch,Fare,Embarked
      --clusters 2
      -n kmodel
      --maxIterations 1000",
  */

//  // Trains a k-means model.
//  val kmeans = new KMeans()
//
//    .setK(2)
//    .setSeed(1L)
//
//  println(kmeans.explainParams)
//
//  val pipeline = new Pipeline()
//    .setStages(Array(kmeans))
//
//  val model = kmeans.fit(titanicTrain)
//
//  // Evaluate clustering by computing Within Set Sum of Squared Errors.
//  val WSSSE = model.computeCost(titanicTrain)
//  println(s"Within Set Sum of Squared Errors = $WSSSE")
//
//  // Shows the result.
//  println("Cluster Centers: ")
//  model.clusterCenters.foreach(println)


}