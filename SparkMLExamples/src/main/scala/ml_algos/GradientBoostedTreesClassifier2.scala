package ml_algos

import ml_algos.GradientBoostedTreesClassifier1.predictions
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object GradientBoostedTreesClassifier2 extends App {
  val path = "src/main/resources/"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("localGradientBoostedTrees1App")
  sparkConf.setMaster("local")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  // Load and parse the data file, converting it to a DataFrame.
  val data = spark.read.format("libsvm").load(path+"sample_libsvm_data.txt")

  println (data.first)


  // Index labels, adding metadata to the label column.
  // Fit on whole dataset to include all labels in index.
  val labelIndexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("indexedLabel")
    .fit(data)
  // Automatically identify categorical features, and index them.
  // Set maxCategories so features with > 4 distinct values are treated as continuous.
  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4)
    .fit(data)

  // Split the data into training and test sets (30% held out for testing).
  val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

  // Train a GBT model.
  val gbt = new GBTClassifier()
    .setLabelCol("indexedLabel")
    .setFeaturesCol("indexedFeatures")
    .setMaxIter(10)

  // Convert indexed labels back to original labels.
  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(labelIndexer.labels)

  // Chain indexers and GBT in a Pipeline.
  val pipeline = new Pipeline()
    .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))


  val labelIndexer1 = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("indexedLabel")
    .fit(data)
  val li1 = labelIndexer1.transform(data)
  li1.printSchema()
  li1.show(15)


  val featureIndexer1 = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4)
    .fit(data)
  val fi1 = featureIndexer1.transform(data)
  fi1.printSchema()
  fi1.show(15)


  println(gbt.explainParams)

  // Train model. This also runs the indexers.
  val model = pipeline.fit(trainingData)

  // Make predictions.
  val predictions = model.transform(testData)

  predictions.printSchema()

  predictions.sample(false, 0.3).show(15)

  predictions.show(15)

  // Select example rows to display.
  predictions.select("predictedLabel", "label", "features").show(5)

  // Select (prediction, true label) and compute test error.
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("indexedLabel")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
  val accuracy = evaluator.evaluate(predictions)
  println("Test Error = " + (1.0 - accuracy))

//  val gbtModel = model.stages(1).asInstanceOf[GBTClassificationModel]
//  println("Learned classification GBT model:\n" + gbtModel.toDebugString)

}