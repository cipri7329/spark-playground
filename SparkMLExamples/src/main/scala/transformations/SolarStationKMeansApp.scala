package transformations

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import utils.{ResourcesUtil, SparkJob}
import org.apache.spark.sql.functions._
import utils.TimeUtil.TimeProfilerUtility._

object SolarStationKMeansApp extends SparkJob{


  def main(args: Array[String]): Unit = {

    var stations = loadSolarStations()
    stations = enhanceSolarStations(stations)

    val centroids = getCentroids(stations)
    initKMeans(centroids, clusters = 98)

//    trainingDataDF()
  }

  def loadSolarStations(): DataFrame = {
    val filePath = "src/main/resources/parquet/solar_station"
    println(s"===READING from: $filePath")
    val df = spark.read.parquet(filePath)
    df
  }

  def enhanceSolarStations(stations_ : DataFrame): DataFrame = {
    var stations = stations_

    stations = stations.withColumn("s_lat", col("lat"))
      .withColumn("s_lon", col("lon"))
      .withColumn("center", array(col("lat"), col("lon") + 360) )
      .withColumn("iid", monotonically_increasing_id)
    stations.cache()
    stations.show()
    stations.printSchema()
    println("total stations")
    stations.count()

    stations
  }

  def getCentroids(stations : DataFrame): Array[Vector] = {
    import org.apache.spark.mllib.linalg.Vectors
    import spark.implicits._

    val centroids = stations.select("center")
      .map{row =>
        val array = row(0).asInstanceOf[Seq[Double]]
        Array[Double](array(0), array(1))
      }
      .collect().map( v => Vectors.dense(v))

    centroids.take(5).foreach(println)
    centroids
  }

  def initKMeans(centroids: Array[Vector], clusters: Int): Unit = {
    val initialModel = new KMeansModel(clusterCenters=centroids)

//    val dataRDD = trainingDataRDD().cache()

    var df = trainingDataDF().cache()

    val data = df.select("position").rdd.map( row => row(0).asInstanceOf[Vector]).cache()

    val k = 98

    val model = new KMeans()
      .setK(k)
      .setInitialModel(initialModel)
      .setMaxIterations(50)
      .run(data)

    val predictionUDF = udf{(p:Vector) => model.predict(p)}
    val centerUDF = udf{ (c:Int) => model.clusterCenters(c)}
    val centerLatUDF = udf( (c:Vector) => c(0))
    val centerLonUDF = udf( (c:Vector) => c(1))

    df = df.withColumn("cluster", predictionUDF(col("position")))
    df = df.withColumn("center", centerUDF(col("cluster")))
    df = df.withColumn("center_lat", centerLatUDF(col("center")))
    df = df.withColumn("center_lon", centerLonUDF(col("center")))

    df.show(false)

    df.collect().foreach(println)

    println(s"distinct clusters: ${df.select("cluster").distinct().count()}")

    println()
    println(s"measurements per cluster: ")
    df.select("cluster").groupBy("cluster").count().orderBy("cluster").collect().foreach(println)

//    val predicted = model.predict(data)
//    val dataZipped = data.zip(predicted)
//    dataZipped.foreach(println)

//    println("predict one by one")
//    data.collect().foreach{ d =>
//      println(s"${d} ${model.predict(d)}")
//      }

//    val centroidsWithPrediction = centroids.zipWithIndex.zip(dataZipped.collect())
//    println("===centroidsWithPrediction")
//    centroidsWithPrediction.foreach(println)
  }


  def trainingDataDF(): DataFrame = {

    val measurements = Seq(
        (39.0, 255.0),
        (35.0, 263.0),
        (36.0, 267.0),
        (34.0, 259.0),
        (31.0, 268.0),
        (34.0, 257.0),
        (36.0, 265.0),
        (37.0, 269.0),
        (39.0, 254.0),
        (31.0, 266.0),
        (35.0, 261.0),
        (36.0, 263.0),
        (37.0, 267.0),
        (35.0, 259.0),
        (31.0, 264.0),
        (37.0, 265.0),
        (35.0, 257.0),
        (36.0, 261.0),
        (31.0, 262.0),
        (38.0, 269.0),
        (32.0, 268.0),
        (36.0, 259.0),
        (38.0, 267.0),
        (37.0, 263.0),
        (31.0, 260.0),
        (38.0, 265.0),
        (31.0, 258.0),
        (39.0, 269.0),
        (32.0, 266.0),
        (36.0, 257.0),
        (37.0, 261.0),
        (32.0, 264.0),
        (31.0, 256.0),
        (38.0, 263.0),
        (33.0, 268.0),
        (37.0, 259.0),
        (39.0, 267.0),
        (39.0, 265.0),
        (38.0, 261.0),
        (31.0, 255.0),
        (37.0, 257.0),
        (32.0, 262.0),
        (33.0, 266.0),
        (34.0, 268.0),
        (32.0, 260.0),
        (38.0, 259.0),
        (31.0, 254.0),
        (33.0, 264.0),
        (39.0, 263.0),
        (38.0, 257.0),
        (33.0, 262.0),
        (34.0, 266.0),
        (39.0, 261.0),
        (32.0, 258.0),
        (34.0, 264.0),
        (32.0, 256.0),
        (35.0, 268.0),
        (39.0, 259.0),
        (33.0, 260.0),
        (34.0, 262.0),
        (39.0, 257.0),
        (32.0, 255.0),
        (33.0, 258.0),
        (35.0, 266.0),
        (31.0, 269.0),
        (33.0, 256.0),
        (35.0, 264.0),
        (34.0, 260.0),
        (32.0, 254.0),
        (36.0, 268.0),
        (36.0, 266.0),
        (33.0, 255.0),
        (31.0, 267.0),
        (34.0, 258.0),
        (35.0, 262.0),
        (31.0, 265.0),
        (37.0, 268.0),
        (33.0, 254.0),
        (34.0, 256.0),
        (35.0, 260.0),
        (36.0, 264.0),
        (31.0, 263.0),
        (35.0, 258.0),
        (36.0, 262.0),
        (37.0, 266.0),
        (34.0, 255.0),
        (37.0, 264.0),
        (35.0, 256.0),
        (32.0, 269.0),
        (38.0, 268.0),
        (34.0, 254.0),
        (36.0, 260.0),
        (31.0, 261.0),
        (31.0, 259.0),
        (38.0, 266.0),
        (35.0, 255.0),
        (37.0, 262.0),
        (36.0, 258.0),
        (32.0, 267.0),
        (31.0, 257.0),
        (33.0, 269.0),
        (32.0, 265.0),
        (37.0, 260.0),
        (39.0, 268.0),
        (36.0, 256.0),
        (35.0, 254.0),
        (38.0, 264.0),
        (36.0, 255.0),
        (32.0, 263.0),
        (33.0, 267.0),
        (37.0, 258.0),
        (39.0, 266.0),
        (38.0, 262.0),
        (33.0, 265.0),
        (32.0, 261.0),
        (34.0, 269.0),
        (36.0, 254.0),
        (39.0, 264.0),
        (38.0, 260.0),
        (37.0, 256.0),
        (39.0, 262.0),
        (34.0, 267.0),
        (37.0, 255.0),
        (38.0, 258.0),
        (32.0, 259.0),
        (33.0, 263.0),
        (33.0, 261.0),
        (37.0, 254.0),
        (35.0, 269.0),
        (32.0, 257.0),
        (34.0, 265.0),
        (39.0, 260.0),
        (38.0, 256.0),
        (38.0, 255.0),
        (34.0, 263.0),
        (35.0, 267.0),
        (33.0, 259.0),
        (39.0, 258.0),
        (36.0, 269.0),
        (39.0, 256.0),
        (33.0, 257.0),
        (38.0, 254.0),
        (35.0, 265.0),
        (34.0, 261.0)
    )

    import spark.implicits._

    val locationUDF = udf( (lat: Double, lon: Double) =>
          Vectors.dense(lat, lon)
      )

    var df = measurements.toDF("lat", "lon")
    df = df.withColumn("position", locationUDF(col("lat"), col("lon")))

    df.show()
    df.printSchema()
    df
  }

  def trainingDataRDD(): RDD[Vector] = {
    val weatherMeasurements = Seq(
      Vectors.dense(39.0, 255.0),
      Vectors.dense(35.0, 263.0),
      Vectors.dense(36.0, 267.0),
      Vectors.dense(34.0, 259.0),
      Vectors.dense(31.0, 268.0),
      Vectors.dense(34.0, 257.0),
      Vectors.dense(36.0, 265.0),
      Vectors.dense(37.0, 269.0),
      Vectors.dense(39.0, 254.0),
      Vectors.dense(31.0, 266.0),
      Vectors.dense(35.0, 261.0),
      Vectors.dense(36.0, 263.0),
      Vectors.dense(37.0, 267.0),
      Vectors.dense(35.0, 259.0),
      Vectors.dense(31.0, 264.0),
      Vectors.dense(37.0, 265.0),
      Vectors.dense(35.0, 257.0),
      Vectors.dense(36.0, 261.0),
      Vectors.dense(31.0, 262.0),
      Vectors.dense(38.0, 269.0),
      Vectors.dense(32.0, 268.0),
      Vectors.dense(36.0, 259.0),
      Vectors.dense(38.0, 267.0),
      Vectors.dense(37.0, 263.0),
      Vectors.dense(31.0, 260.0),
      Vectors.dense(38.0, 265.0),
      Vectors.dense(31.0, 258.0),
      Vectors.dense(39.0, 269.0),
      Vectors.dense(32.0, 266.0),
      Vectors.dense(36.0, 257.0),
      Vectors.dense(37.0, 261.0),
      Vectors.dense(32.0, 264.0),
      Vectors.dense(31.0, 256.0),
      Vectors.dense(38.0, 263.0),
      Vectors.dense(33.0, 268.0),
      Vectors.dense(37.0, 259.0),
      Vectors.dense(39.0, 267.0),
      Vectors.dense(39.0, 265.0),
      Vectors.dense(38.0, 261.0),
      Vectors.dense(31.0, 255.0),
      Vectors.dense(37.0, 257.0),
      Vectors.dense(32.0, 262.0),
      Vectors.dense(33.0, 266.0),
      Vectors.dense(34.0, 268.0),
      Vectors.dense(32.0, 260.0),
      Vectors.dense(38.0, 259.0),
      Vectors.dense(31.0, 254.0),
      Vectors.dense(33.0, 264.0),
      Vectors.dense(39.0, 263.0),
      Vectors.dense(38.0, 257.0),
      Vectors.dense(33.0, 262.0),
      Vectors.dense(34.0, 266.0),
      Vectors.dense(39.0, 261.0),
      Vectors.dense(32.0, 258.0),
      Vectors.dense(34.0, 264.0),
      Vectors.dense(32.0, 256.0),
      Vectors.dense(35.0, 268.0),
      Vectors.dense(39.0, 259.0),
      Vectors.dense(33.0, 260.0),
      Vectors.dense(34.0, 262.0),
      Vectors.dense(39.0, 257.0),
      Vectors.dense(32.0, 255.0),
      Vectors.dense(33.0, 258.0),
      Vectors.dense(35.0, 266.0),
      Vectors.dense(31.0, 269.0),
      Vectors.dense(33.0, 256.0),
      Vectors.dense(35.0, 264.0),
      Vectors.dense(34.0, 260.0),
      Vectors.dense(32.0, 254.0),
      Vectors.dense(36.0, 268.0),
      Vectors.dense(36.0, 266.0),
      Vectors.dense(33.0, 255.0),
      Vectors.dense(31.0, 267.0),
      Vectors.dense(34.0, 258.0),
      Vectors.dense(35.0, 262.0),
      Vectors.dense(31.0, 265.0),
      Vectors.dense(37.0, 268.0),
      Vectors.dense(33.0, 254.0),
      Vectors.dense(34.0, 256.0),
      Vectors.dense(35.0, 260.0),
      Vectors.dense(36.0, 264.0),
      Vectors.dense(31.0, 263.0),
      Vectors.dense(35.0, 258.0),
      Vectors.dense(36.0, 262.0),
      Vectors.dense(37.0, 266.0),
      Vectors.dense(34.0, 255.0),
      Vectors.dense(37.0, 264.0),
      Vectors.dense(35.0, 256.0),
      Vectors.dense(32.0, 269.0),
      Vectors.dense(38.0, 268.0),
      Vectors.dense(34.0, 254.0),
      Vectors.dense(36.0, 260.0),
      Vectors.dense(31.0, 261.0),
      Vectors.dense(31.0, 259.0),
      Vectors.dense(38.0, 266.0),
      Vectors.dense(35.0, 255.0),
      Vectors.dense(37.0, 262.0),
      Vectors.dense(36.0, 258.0),
      Vectors.dense(32.0, 267.0),
      Vectors.dense(31.0, 257.0),
      Vectors.dense(33.0, 269.0),
      Vectors.dense(32.0, 265.0),
      Vectors.dense(37.0, 260.0),
      Vectors.dense(39.0, 268.0),
      Vectors.dense(36.0, 256.0),
      Vectors.dense(35.0, 254.0),
      Vectors.dense(38.0, 264.0),
      Vectors.dense(36.0, 255.0),
      Vectors.dense(32.0, 263.0),
      Vectors.dense(33.0, 267.0),
      Vectors.dense(37.0, 258.0),
      Vectors.dense(39.0, 266.0),
      Vectors.dense(38.0, 262.0),
      Vectors.dense(33.0, 265.0),
      Vectors.dense(32.0, 261.0),
      Vectors.dense(34.0, 269.0),
      Vectors.dense(36.0, 254.0),
      Vectors.dense(39.0, 264.0),
      Vectors.dense(38.0, 260.0),
      Vectors.dense(37.0, 256.0),
      Vectors.dense(39.0, 262.0),
      Vectors.dense(34.0, 267.0),
      Vectors.dense(37.0, 255.0),
      Vectors.dense(38.0, 258.0),
      Vectors.dense(32.0, 259.0),
      Vectors.dense(33.0, 263.0),
      Vectors.dense(33.0, 261.0),
      Vectors.dense(37.0, 254.0),
      Vectors.dense(35.0, 269.0),
      Vectors.dense(32.0, 257.0),
      Vectors.dense(34.0, 265.0),
      Vectors.dense(39.0, 260.0),
      Vectors.dense(38.0, 256.0),
      Vectors.dense(38.0, 255.0),
      Vectors.dense(34.0, 263.0),
      Vectors.dense(35.0, 267.0),
      Vectors.dense(33.0, 259.0),
      Vectors.dense(39.0, 258.0),
      Vectors.dense(36.0, 269.0),
      Vectors.dense(39.0, 256.0),
      Vectors.dense(33.0, 257.0),
      Vectors.dense(38.0, 254.0),
      Vectors.dense(35.0, 265.0),
      Vectors.dense(34.0, 261.0)
    )

    val data: RDD[Vector] = spark.sparkContext.parallelize(weatherMeasurements)
    data
  }

}
