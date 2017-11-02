package ml_algos.estimators

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object NNGram1 extends App {
  val path = "src/main/resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localGradientBoostedTrees1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // Load and parse the data file, converting it to a DataFrame.
//  val data = spark.read.format("libsvm").load(path+"sample_libsvm_data.txt")
//  println (data.first)


  import org.apache.spark.ml.feature.NGram

  val wordDataFrame = spark.createDataFrame(Seq(
    (0, Array("Hi", "I", "heard", "about", "Spark")),
    (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
    (2, Array("Logistic", "regression", "models", "are", "neat")),
    (3, Array("Logistic1", "regression2", "models3")),
    (4, Array("Logistic1a", "regression2a")),
    (5, Array("Logistic1b"))
  )).toDF("id", "words")

  val ngram = new NGram().setN(3).setInputCol("words").setOutputCol("ngrams")

  val ngramDataFrame = ngram.transform(wordDataFrame)
  ngramDataFrame.select("ngrams").show(false)
}