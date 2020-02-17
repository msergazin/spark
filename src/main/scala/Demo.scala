import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

import scala.util.Random

object Demo extends App {

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("movies")
    .set("spark.driver.host", "localhost")
  val sc: SparkContext = new SparkContext(conf)
  val movies = sc.textFile(Data.moviesFilePath).map(line => {
    val fields = line.split("::")
    // format: (movieId, movieName)
    (fields(0).toInt, fields(1))
  }).collect.toMap

  val ratings = sc.textFile(Data.ratingsFilePath).map { line =>
    val fields = line.split("::")
    // format: (timestamp % 10, Rating(userId, movieId, rating))
    (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
  }

  println("moveis: " + movies.size)
  val numRatings = ratings.count
  val numUsers = ratings.map(_._2.user).distinct.count
  val numMovies = ratings.map(_._2.product).distinct.count

  val mostRatedMovieIds = ratings.map(_._2.product) // extract movie ids
    .countByValue      // count ratings per movie
    .toSeq             // convert map to Seq
    .sortBy(- _._2)    // sort by rating count
    .take(50)          // take 50 most rated
    .map(_._1)         // get their ids

  println(mostRatedMovieIds)
  mostRatedMovieIds.foreach(movie => println(movies(movie)))
  val random = new Random(0)
  val selectedMovies = mostRatedMovieIds.filter(x => random.nextDouble() < 0.2)
    .map(x => (x, movies(x)))
    .toSeq
  selectedMovies.foreach(println)
}
