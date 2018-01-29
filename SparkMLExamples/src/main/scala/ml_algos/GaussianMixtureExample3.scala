package ml_algos

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql._

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object GaussianMixtureExample3 extends App {
  val path = "src/main/resources/"

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localGaussianMixtureExample1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  // Load and parse the data file, converting it to a DataFrame.
  import spark.implicits._


  // Loads data.
    val dataset = spark.read.format("libsvm").load(path+"sample_libsvm_data.txt")
//  val dataset = spark.read.format("libsvm").load(path+"sample_kmeans_data.txt")


  dataset.printSchema()

  // Trains Gaussian Mixture Model
  val gmm = new GaussianMixture()
    .setK(3)

  println(gmm.explainParams())

  val model = gmm.fit(dataset)

  val predictions = model.transform(dataset)

  predictions.show(false)


  // output parameters of mixture model model
  for (i <- 0 until model.getK) {
    println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
      s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
  }

}