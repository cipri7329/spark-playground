package ml_algos.estimators

import org.apache.spark.SparkConf
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, VectorIndexer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object VectorIndexer1 extends App {
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
  val data = spark.read.format("libsvm").load(path+"sample_libsvm_data.txt")

  println (data.first)


  /*
  VectorIndexer helps index categorical features in datasets of Vectors.
  It can both automatically decide which features are categorical and convert original values to category indices.

  Specifically, it does the following:

    1. Take an input column of type Vector and a parameter maxCategories.

    2. Decide which features should be categorical based on the number of distinct values,
    where features with at most maxCategories are declared categorical.

    3. Compute 0-based category indices for each categorical feature.

    4. Index categorical features and transform original feature values to indices.

  Indexing categorical features allows algorithms such as Decision Trees and Tree Ensembles to treat categorical features appropriately,
  improving performance.
   */

  val indexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexed")
    .setMaxCategories(10)

  val indexerModel = indexer.fit(data)

  val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
  println(s"Chose ${categoricalFeatures.size} categorical features: " +
    categoricalFeatures.mkString(", "))

  // Create new column "indexed" with categorical values transformed to indices
  val indexedData = indexerModel.transform(data)
  indexedData.show()

  println(s"first line ${indexedData.first()}")

  val s1: SparseVector = indexedData.first().getAs[SparseVector](1)
  val s2: SparseVector = indexedData.first().getAs[SparseVector](2)


  println(s1.size)
  println(s2.size)

}