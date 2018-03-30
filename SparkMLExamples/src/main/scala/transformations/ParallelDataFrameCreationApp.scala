package transformations

import utils.SparkJob

object ParallelDataFrameCreationApp extends SparkJob{

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    (1 to 100).par.foreach { _ =>
      println(spark.sparkContext.parallelize(1 to 5).map { i => (i, i) }.toDF("a", "b").count())
    }

  }

}
