package ml_algos.estimators

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.attribute.Attribute
/**
  * https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier
  *
  * examples/src/main/scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala
  */


object StringIndexer1 extends App {
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


  val rdd = ('a' to 'a' + 9).map(_.toString)
    .zip(0 to 9)
    .map(_.swap)
  val df = spark.createDataFrame(rdd).toDF("id", "label")


  import org.apache.spark.ml.feature.StringIndexer
  val strIdx = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("index")

  /*
    StringIndexer encodes a string column of labels to a column of label indices.

    The indices are in [0, numLabels), ordered by label frequencies, so the most frequent label gets index 0.
    The unseen labels will be put at index numLabels if user chooses to keep them.
    If the input column is numeric, we cast it to string and index the string values.

    When downstream pipeline components such as Estimator or Transformer make use of this string-indexed label,
    you must set the input column of the component to this string-indexed column name.
   */


  println(strIdx.explainParams)

  val model = strIdx.fit(df)
  val indexed = model.transform(df)

  indexed.show
  indexed.printSchema

  val df1 = spark.createDataFrame(
    Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
  ).toDF("id", "category")

  val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")

  val indexed2 = indexer.fit(df1).transform(df1)
  indexed2.show()

  /*
      +---+--------+-------------+
    | id|category|categoryIndex|
    +---+--------+-------------+
    |  0|       a|          0.0|
    |  1|       b|          2.0|
    |  2|       c|          1.0|
    |  3|       a|          0.0|
    |  4|       a|          0.0|
    |  5|       c|          1.0|
    +---+--------+-------------+
   */

  val df2 = spark.createDataFrame(
      ('a' to 'a' + 9).map(_.toString)
      .zip(0 to 9)
      .map(_.swap))
    .toDF("id", "category")
  val df3 = spark.createDataFrame(
    Seq((0, "b"), (1, "a"), (2, "e"), (3, "a"), (4, "b"), (5, "c"))
  ).toDF("id", "category")


  val indexer2 = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")
    .setHandleInvalid("skip") //"keep" available in 2.2

  val model2 = indexer2.fit(df1)
  model2.transform(df1).show()

  val indexed3 = model2.transform(df2)
  indexed3.show()

  val indexed4 = model2.transform(df3)
  indexed4.show()



  /* IndexToString
    Symmetrically to StringIndexer
    IndexToString maps a column of label indices back to a column containing the original labels as strings.

    A common use case is to produce indices from labels with StringIndexer,
    train a model with those indices and
    retrieve the original labels from the column of predicted indices with IndexToString.

    However, you are free to supply your own labels.
   */

  val inputColSchema = indexed4.schema(indexer2.getOutputCol)
  println(s"StringIndexer will store labels in output column metadata: " +
    s"${Attribute.fromStructField(inputColSchema).toString}\n")

  println("model2 labels: " + model2.labels)
  for(label <- model2.labels)
    println(label)

  val converter = new IndexToString()
    .setInputCol("categoryIndex")
    .setOutputCol("originalCategory")
    .setLabels(Array("a1", "c1", "b1", "d1"))

  val converted = converter.transform(indexed4)

  println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
    s"column '${converter.getOutputCol}' using labels in metadata")
  converted.select("id", "categoryIndex", "originalCategory").show()

  converted.show()

  /*
      +---+--------+-------------+----------------+
    | id|category|categoryIndex|originalCategory|
    +---+--------+-------------+----------------+
    |  0|       b|          2.0|              b1|
    |  1|       a|          0.0|              a1|
    |  3|       a|          0.0|              a1|
    |  4|       b|          2.0|              b1|
    |  5|       c|          1.0|              c1|
    +---+--------+-------------+----------------+
   */

  /*
    StringToIndex
    - for labels generates indices based on the frequency of the labels in the dataset
    - 0 represents the most frequent label

    IndexToString
    - for generated indices, set again string labels
    - supports setting new labels

    A common use case is to produce indices from labels with StringIndexer,
    train a model with those indices and
    retrieve the original labels from the column of predicted indices with IndexToString.
  */
}