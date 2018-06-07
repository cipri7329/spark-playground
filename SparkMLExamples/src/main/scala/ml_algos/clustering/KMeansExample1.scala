package ml_algos.clustering

import org.apache.spark.ml.clustering.KMeans
import utils.SparkJob

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object KMeansExample1 extends SparkJob {
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

  // Loads data.
  val dataset = spark.read.format("libsvm").load(path+"sample_kmeans_data.txt")


  // Trains a k-means model.
  val kmeans = new KMeans().setK(2).setSeed(1L)
  val model = kmeans.fit(dataset)

  // Evaluate clustering by computing Within Set Sum of Squared Errors.
  val WSSSE = model.computeCost(dataset)
  println(s"Within Set Sum of Squared Errors = $WSSSE")

  // Shows the result.
  println("Cluster Centers: ")
  model.clusterCenters.foreach(println)

}