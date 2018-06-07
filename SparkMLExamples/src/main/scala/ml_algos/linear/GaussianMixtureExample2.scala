package ml_algos.linear

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector
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

//  val probabilityToList = udf( (id:Long, predictions: DenseVector) => {
//    val membershipValues = predictions.toArray.zipWithIndex.sortBy(_._1)(Ordering[Double].reverse)
//    val topMemberships = membershipValues.take(3)
//      .toList.flatMap { case (value: Double, clusterId: Int) => List(clusterId, value) }
//    topMemberships
//  })

  val probabilityToList = udf( (predictions: DenseVector) => {
    val membershipValues = predictions.toArray.zipWithIndex.sortBy(_._1)(Ordering[Double].reverse)
    val topMemberships = membershipValues.take(3)
      .toList.flatMap { case (value: Double, clusterId: Int) => List(clusterId, value) }
    topMemberships
  })

  predictions.show(false)

//  val df_filtered = predictionsDF.withColumn("result", probabilityToList(col("probability")))
//  val df_result = df_filtered.select("label", "result")

  val rdd2T = predictions.select("PassengerId", "probability").rdd

  val rddResult = rdd2T
    .map{ row => (row.getAs[Int](0).toLong, row.getAs[DenseVector](1).values)}
    .map{ case (id:Long, values:Array[Double]) =>
//      val vector = Vectors.dense(values)
      val membershipValues = values.zipWithIndex.sortBy(_._1)(Ordering[Double].reverse)
      val topMemberships = membershipValues.take(3)
        .toList.flatMap { case (value: Double, clusterId: Int) => List[Any](clusterId, value) }
      (id, topMemberships)
    }

  rddResult.take(10).foreach(println)

//  val df_filtered = predictions.withColumn("result", probabilityToList(col("probability")))
//
//  df_filtered.show(false)
//
//  val df_result = df_filtered.select("PassengerId", "result")
//
//  df_result.printSchema()
//
////  val rdd2 = df_result.rdd.map{ case Row(id:Int, values:Array[Double]) => (result, values)}
////  val rdd2 = df_result.rdd.map{ row => (row.getAs[Int](0), row.getAs[mutable.WrappedArray[Double]](1).toArray[Double])}
//  val rdd2 = df_result.rdd.map{ row => (row.getAs[Long](0), row.getAs[mutable.WrappedArray[Any]](1).toArray[Any].toList)}
//
//  rdd2.take(10).foreach(println)

  val gmmModel = model.stages(1).asInstanceOf[GaussianMixtureModel]

  // output parameters of mixture model model
  for (i <- 0 until gmmModel.getK) {
    println(s"Gaussian $i:\nweight=${gmmModel.weights(i)}\n" +
      s"mu=${gmmModel.gaussians(i).mean}\nsigma=\n${gmmModel.gaussians(i).cov}\n")
  }

}