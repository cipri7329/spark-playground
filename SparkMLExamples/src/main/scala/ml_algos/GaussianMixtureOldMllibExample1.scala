package ml_algos

import ml_algos.GaussianMixtureExample1.spark
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object GaussianMixtureOldMllibExample1 extends App {
  val path = "src/main/resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localGaussianMixtureExample1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  // Load and parse the data file, converting it to a DataFrame.
  //  val data = spark.read.format("libsvm").load(path+"sample_libsvm_data.txt")
  //  println (data.first)

  // Loads data.
//  val dataset = spark.read.format("libsvm").load(path+"sample_kmeans_data.txt")
  import spark.implicits._

  // Load and parse the data
  val data = spark.sparkContext.textFile(path+"gmm_data.txt")

  val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()

  // Cluster the data into two classes using GaussianMixture
  val gmm = new GaussianMixture().setK(2).run(parsedData)



  // Save and load model
  gmm.save(spark.sparkContext, path)
  val sameModel = GaussianMixtureModel.load(spark.sparkContext,path)

  // output parameters of max-likelihood model
  for (i <- 0 until gmm.k) {
    println("weight=%f\nmu=%s\nsigma=\n%s\n" format
      (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
  }

}