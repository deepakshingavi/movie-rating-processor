package com.dp.training


import com.dp.training.model.{Movie, Rating, User}
import com.dp.training.usecase.BusinessAgg
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
/**
 * Entry class for Main
 */

object Main {
  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    val inputDir = params.getOrElse("input", throw new RuntimeException("Parameter 'input' not found"))
    val outputDir = params.getOrElse("output", throw new RuntimeException("Parameter 'output' not found"))
    val startTime = System.nanoTime()
    implicit val spark: SparkSession = SparkSessionProvider.initSparkSession("local[*]")

    val movieData: DataFrame = BusinessAgg
      .loadMovieData[Movie](inputDir + "movies.dat")(Encoders.product[Movie],spark)

    val userData: DataFrame = BusinessAgg
      .loadData[User](inputDir + "users.dat")(Encoders.product[User],spark)

    val ratingData: DataFrame = BusinessAgg.loadData[Rating](inputDir + "ratings.dat")(Encoders.product[Rating],spark)

    val ratingAggregatedData = BusinessAgg.ratingAgg(ratingData)

    BusinessAgg.writeRatingAggDataByMovie(ratingAggregatedData, outputDir,"agg_movie_ratings")

    val topRatedMovieByUser = BusinessAgg.getLatestTop3RateMoviesForUser(ratingData)

    BusinessAgg.writeTopRatedMovieByUser(topRatedMovieByUser, outputDir,"top_rated_movie_per_user")

    BusinessAgg.writeMovieData(movieData,outputDir,"movie")

    BusinessAgg.writeUserData(userData,outputDir,"user")

//    Records are too less to be saved in daily partition hence saved them to year partition so that look for
    //    record can be shortened
    BusinessAgg.writeRatingData(ratingData,outputDir,"rating")

    val endTime = System.nanoTime()
    val executionTime = (endTime - startTime) / (1000 * 1e6) // Convert nanoseconds to milliseconds
    println(s"Execution time: $executionTime seconds")

  }

  def parseArgs(args: Array[String]): Map[String, String] = {
    args.sliding(2, 2).collect {
      case Array(key, value) => key.stripPrefix("--") -> value
    }.toMap
  }

}