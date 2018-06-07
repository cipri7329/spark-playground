package ml_algos.linear

import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql._
import utils.SparkJob

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object GaussianMixtureExample1 extends SparkJob {
  val path = "src/main/resources/"

  import org.apache.spark.sql.functions._

  // Load and parse the data file, converting it to a DataFrame.
  //  val data = spark.read.format("libsvm").load(path+"sample_libsvm_data.txt")
  //  println (data.first)
  import spark.implicits._

  val sexProjection: Column = when($"tesex" === 1, "male").otherwise("female").as("sex")


  // Loads data.
  val dataset = spark.read.format("libsvm").load(path+"sample_kmeans_data.txt")

  val ageProjection: Column = {
    when($"label" >= 5 && $"label" <= 22, "young")
      .when($"label" >= 2 && $"label" < 5, "active")
      .otherwise("elder")
      .as("age")
  }

  val newDataset = dataset.withColumn("age", ageProjection)

  dataset.printSchema()
  newDataset.printSchema()

  newDataset.show()

  // Trains Gaussian Mixture Model
  val gmm = new GaussianMixture()
    .setK(3)

  println(gmm.explainParams())

  val model = gmm.fit(dataset)
  val model2 = gmm.fit(newDataset)

  val predictions = model.transform(dataset)
  val predictions2 = model2.transform(newDataset)



  predictions.show(false)
  predictions2.show(false)



  // output parameters of mixture model model
  for (i <- 0 until model.getK) {
    println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
      s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
  }

}