package ml_algos.clustering

import org.apache.spark.ml.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import utils.{ResourcesUtil, SparkJob}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Spark LDA example using LDA ML (new ML lib)
  */
object LdaML2Ex1 extends SparkJob{

  def main(args : Array[String]): Unit = {

    val data = getData()
    data.show(false)
    data.printSchema()

    val opts = SparkLdaTrainingOptions2(
      name = "summer",
      wordsColumn = "text",
      topics = 5,
      max_iterations = 10,
      termsPerTopic = 10,
      doc_concentration = Array[Double](1.3, 1.3, 1.3, 1.3, 1.3),
      topic_concentration = 1.02,
      vocab_size = 1000,
      algorithm = "em",
      seed = 1,
      stopwords  = "",
      checkpointInterval = 10,
      docIdColumn = "id"
    )

    import spark.implicits._
    val (corpus: RDD[(Long, Vector)], vocabArray) = preprocess(opts, data)
    val trainingDataDF: DataFrame = corpus.toDF("label", "features")

    println("===corpus===")
    corpus.take(10).foreach(println)

    println("===vocabArray===")
    println()
    vocabArray.foreach( s => print(f"${s}%s, "))
    println()
    println(s"===vocabArray size: ${vocabArray.length} ===")


//    // Set LDA options from SparkLdaOptions
    val ldaAlgo = new LDA()
      .setK(opts.topics)
      .setMaxIter(opts.max_iterations)
      .setCheckpointInterval(opts.checkpointInterval)
      .setDocConcentration(opts.doc_concentration)
      .setTopicConcentration(opts.topic_concentration)
      .setOptimizer(opts.algorithm)
      .setSeed(opts.seed)

    val ldaModel = ldaAlgo.fit(trainingDataDF)
      .asInstanceOf[DistributedLDAModel]

    // Build the predictions table
    var predictionsTableData = new ArrayBuffer[List[Any]]
    val allWordTopics = ldaModel.describeTopics()

    println("===allWordTopics -> describeTopics===")
    allWordTopics.show(false)
    allWordTopics.printSchema()
    println(s"===allWordTopics: ${allWordTopics.count()}===")

    for (z <- vocabArray.indices) {
      // Search each topic and add this word to the predictions array
      var currentWordPredictions : ArrayBuffer[Any] = new ArrayBuffer()
      currentWordPredictions += vocabArray(z)
      val wordPredAcc = spark.sparkContext.collectionAccumulator[Double]("wordPredictions")
      allWordTopics.foreach{x => {
          val termWeights = x.getAs[Seq[Double]](2)
          val termIndices = x.getAs[Seq[Int]](1)
          val termIndex = termIndices.indexOf(z)
          val topicWeight = if (termIndex != -1) termWeights(termIndex) else 0.0
          wordPredAcc.add(topicWeight)
        }
      }
      val wordPredictions = (currentWordPredictions ++ wordPredAcc.value.toArray).toList
      predictionsTableData += wordPredictions
    }

    println("===predictionsTableData===")
    println()
    predictionsTableData.foreach( s => print(f"${s.slice(1, s.length).map(s => s.toString.toDouble).sum}%f ${s}%s\n, "))
    println()
    println(s"===predictionsTableData size: ${predictionsTableData.length} ===")

    val tr = ldaModel.transform(trainingDataDF)
    tr.show(false)

    println("filtered")
    tr.filter(col("label") === "481496872704413697" || col("label") === "481496765460643841" ).show(false)

    val topicsData = tr.rdd.map( row => row.get(0) :: row.getAs[DenseVector]("topicDistribution").toArray.toList.asInstanceOf[List[Any]])

    println("topicsData")
    topicsData.take(10).foreach(s => println(s.toString))

    // Output a topics table with top-weighted terms
    val outputBuffer2 = ldaModel.describeTopics(opts.termsPerTopic).rdd.flatMap{
       row => {
        val topic = row.getAs[Int](0)
        val termIndices = row.getAs[Seq[Int]](1).toList
        val termWeights = row.getAs[Seq[Double]](2).toList
        val weightedTerms: List[(String, Double)] = termIndices.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight)  }
        val topicWeightedTerm = weightedTerms.map{ case (term, weight) => List(topic, term, weight)}
        topicWeightedTerm
      }
    }.collect()

    println("===outputBuffer===")
    outputBuffer2.foreach(println)
  }


  private def getData(): DataFrame = {
    val path = ResourcesUtil.resourcePath("/summer.csv")
    val df = this.spark.read
      .format("csv")
      .option("header", true)
      .load(path)
    df
  }


  /**
    * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
    * @return corpus as RDD[(Long, Vector)]
    */
  private def preprocess(opts : SparkLdaTrainingOptions2, data: DataFrame): (RDD[(Long, Vector)], Array[String]) = {
    // Get the desired column from the database

    val corpus_temp = data.select(opts.docIdColumn, opts.wordsColumn).filter( data(opts.wordsColumn).isNotNull) //select the documents with text != null

    val corpus: RDD[(Long, String)] = corpus_temp.rdd.map(row => (row.getString(0).toLong, row.getString(1)))

    val stopwordsArray = opts.stopwords.split(',').filter(_.nonEmpty)

    val tokenizer = new SimpleTokenizer2(stopwordsArray)
    // Split text into words
    val tokenized: RDD[(Long, IndexedSeq[String])] = corpus.map { case (id, text) =>
      id -> tokenizer.getWords(text)
    }
    tokenized.cache()

    // Counts words: RDD[(word, wordCount)]
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()
    val fullVocabSize = wordCounts.count()
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)
    val (vocab: Map[String, Int], selectedTokenCount: Long) = {
      val tmpSortedWC: Array[(String, Long)] = if (opts.vocab_size == -1 || fullVocabSize <= opts.vocab_size) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2) // Get rid of this collect
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(opts.vocab_size)
      }
      (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
    }
    // Unpersist, as this is not used anymore
    wordCounts.unpersist(blocking = false)

    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = Vectors.sparse(vocab.size, indices, values)
      (id, sb)
    }
    tokenized.unpersist(blocking = false)

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }

    (documents, vocabArray)
  }

}

private case class SparkLdaTrainingOptions2(
                                    name: String,
                                    wordsColumn: String,
                                    topics: Int,
                                    max_iterations: Int = 10,
                                    termsPerTopic: Int = 10,
                                    doc_concentration: Array[Double] = Array[Double](0.0),
                                    topic_concentration: Double = 0.0,
                                    vocab_size: Int = 1000,
                                    algorithm: String = "em",
                                    seed: Long = 1,
                                    stopwords: String = "",
                                    checkpointInterval: Int = 10,
                                    docIdColumn: String = "id") {
}

/**
  * Simple Tokenizer.
  */
private class SimpleTokenizer2(stopWords: Array[String]) extends Serializable {

  private val stopwords: Set[String] = if (stopWords.isEmpty) {
    Set.empty[String]
  } else {
    stopWords.flatMap(_.stripMargin.split("\\s+")).toSet
  }

  // Matches sequences of Unicode letters
  private val allWordRegex = "^(\\p{L}*)$".r

  // Ignore words shorter than this length.
  private val minWordLength = 3

  /**
    * Splits each document in words
    * @param text The text in the document
    * @return Array of words
    */
  def getWords(text: String): IndexedSeq[String] = {
    text.split("[\\.,\\s!;?:\"']+").map(_.toLowerCase).filter(x => x.matches("^(\\p{L}*)$") && x.length >= minWordLength && !stopwords.contains(x))
  }
}
