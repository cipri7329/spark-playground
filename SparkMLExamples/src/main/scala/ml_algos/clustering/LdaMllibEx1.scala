package ml_algos.clustering

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import utils.{ResourcesUtil, SparkJob}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Spark LDA example using LDA Mllib (old ML lib)
  */
object LdaMllibEx1 extends SparkJob{

  def main(args : Array[String]): Unit = {

    val data = getData()
    data.show(false)
    data.printSchema()

    val opts = SparkLdaTrainingOptions(
      name = "summer",
      wordsColumn = "text",
      topics = 5,
      max_iterations = 10,
      termsPerTopic = 10,
      doc_concentration = -1,
      topic_concentration = -1,
      vocab_size = 1000,
      algorithm = "em",
      seed = 1,
      stopwords  = "",
      checkpointInterval = 10,
      docIdColumn = "id"
    )

    val (corpus, vocabArray) = preprocess(opts, data)

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
      .setMaxIterations(opts.max_iterations)
      .setCheckpointInterval(opts.checkpointInterval)
      .setDocConcentration(opts.doc_concentration)
      .setTopicConcentration(opts.topic_concentration)
      .setOptimizer(opts.algorithm)
      .setSeed(opts.seed)

    val ldaModel = ldaAlgo.run(corpus).asInstanceOf[DistributedLDAModel]

    // Build the predictions table
    var predictionsTableData = new ArrayBuffer[List[Any]]
    val allWordTopics = ldaModel.describeTopics()

    println("===allWordTopics===")
    println()
    allWordTopics.foreach(x => println(s"${x._1.take(15).toList} ${x._2.take(15).toList}"))
    println()
    allWordTopics.foreach(x => println(s"${x._1.length} ${x._2.length}"))
    println()

    for (z <- vocabArray.indices) {
      // Search each topic and add this word to the predictions array
      var currentWordPredictions : ArrayBuffer[Any] = new ArrayBuffer()
      currentWordPredictions += vocabArray(z)
      allWordTopics.foreach(x => currentWordPredictions += x._2(x._1.indexOf(z)))
      predictionsTableData += currentWordPredictions.toList
    }

    println("===predictionsTableData===")
    println()
//    predictionsTableData.foreach( s => print(f"${s}%s, "))
    predictionsTableData.foreach( s => print(f"${s.slice(1, s.length).map(s => s.toString.toDouble).sum}%f ${s}%s\n, "))
    println()
    println(s"===predictionsTableData size: ${predictionsTableData.length} ===")

//    val predictionsMetaData = %|%(
//      %("Word", STRING) :: 1.to(opts.topics).map(index => %(s"Topic_$index", DOUBLE)).toList
//    )
//
//    val predictionsModel = SeqDataset(predictionsMetaData, predictionsTableData)
//    logInfo(s"Built predictions table. Metadata is: $predictionsMetaData. Persisting.")
//    TableManager.persist(predictionsModel, s"${opts.name}_predictions")
//

    println("===topTopicsPerDocument===")
    ldaModel.topTopicsPerDocument(opts.topics).take(10).foreach{
      case (docId, topicIndexes, topicWeights) =>
        println(s"$docId ${topicIndexes.toList} ${topicWeights.toList} ${topicWeights.toList.sum}")
    }
    println()

    //Build topics table
    val topicsData: RDD[List[Any]] = ldaModel.topTopicsPerDocument(opts.topics).map {
      case (docId, topicIndexes, topicWeights) =>
        topicIndexes.zip(topicWeights)
          .sortBy(_._1)
          .map { case (topic, weight) => (docId, weight)}
          .groupBy(_._1)
          .mapValues(indexedWeights => indexedWeights.map(_._2))
    }.map(x => x.head._1 :: x.head._2.toList.asInstanceOf[List[Any]])


    //topic id: 481496872704413697 481496765460643841
    println("===topicsData===")
    topicsData.take(10).foreach(println)
    println()
//    val topicsMetaData = %|%(
//      %("Document", LONG) :: 1.to(opts.topics).map(index => %(s"Topic_$index", DOUBLE)).toList
//    )

//    val topicsModel = TableDataset(topicsData, topicsMetaData, limited = false)
//    logInfo(s"Built topics table. Metadata is: $topicsMetaData. Persisting.")
//    TableManager.persist(topicsModel, s"${opts.name}_topics")

    // Output a topics table with top-weighted terms
    val topicIndices: Array[(Array[Int], Array[Double])] = ldaModel.describeTopics(opts.termsPerTopic)
    val topics: Array[Array[(String, Double)]] = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
    val outputBuffer: Array[List[Any]] = topics.zipWithIndex.flatMap { case (topic, index) => topic.map { case (term, weight) => List(index, term, weight) } }

    //    SeqDataset(%|%(%("Topic", INT), %("Word", STRING), %("Weight", DOUBLE)), outputBuffer)
    println("===outputBuffer===")
    outputBuffer.foreach(println)
    println(s"outputBuffer count: ${outputBuffer.size}")
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
  private def preprocess(opts : SparkLdaTrainingOptions, data: DataFrame): (RDD[(Long, Vector)], Array[String]) = {
    // Get the desired column from the database

    val corpus_temp = data.select(opts.docIdColumn, opts.wordsColumn).filter( data(opts.wordsColumn).isNotNull) //select the documents with text != null

    val corpus: RDD[(Long, String)] = corpus_temp.rdd.map(row => (row.getString(0).toLong, row.getString(1)))

    val stopwordsArray = opts.stopwords.split(',').filter(_.nonEmpty)

    val tokenizer = new SimpleTokenizer(stopwordsArray)
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

private case class SparkLdaTrainingOptions(
                                    name: String,
                                    wordsColumn: String,
                                    topics: Int,
                                    max_iterations: Int = 10,
                                    termsPerTopic: Int = 10,
                                    doc_concentration: Double = -1,
                                    topic_concentration: Double = -1,
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
private class SimpleTokenizer(stopWords: Array[String]) extends Serializable {

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
