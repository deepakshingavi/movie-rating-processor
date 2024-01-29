package com.dp.training.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object DataQualityUDF {


  def dataQualityCheckForRating(spark:SparkSession) : UserDefinedFunction = {
    val udf = spark.udf.register("dataQualityUDFForRating", (userID: Int, movieID: Int, rating: Int) => {
      if ((1 to 6040).contains(userID) && (1 to 3952).contains(movieID) && (1 to 5).contains(rating)) "P" else "F"
    })
    udf
  }

  def dataQualityCheckForUser(spark:SparkSession) : UserDefinedFunction = {
    val validAges = Set(1, 18, 25, 35, 45, 50, 56)
    val validGenders = Set("M", "F")
    val validOccupations = 0 to 20

    val udf = spark.udf.register("dataQualityUDFForUser", (age: Int, gender: String, occupation: Int) => {
      val ageIsValid = validAges.contains(age)
      val genderIsValid = validGenders.contains(gender)
      val occupationIsValid = validOccupations.contains(occupation)
      if (ageIsValid && genderIsValid && occupationIsValid) "P" else "F"
    })
    udf
  }

  def dataQualityCheckForMovie(spark:SparkSession) : UserDefinedFunction = {
    val MOVIE_GENRE_SEQ: Seq[String] = Seq(
      "Action",
      "Adventure",
      "Animation",
      "Children's",
      "Comedy",
      "Crime",
      "Documentary",
      "Drama",
      "Fantasy",
      "Film-Noir",
      "Horror",
      "Musical",
      "Mystery",
      "Romance",
      "Sci-Fi",
      "Thriller",
      "War",
      "Western"
    )
    val udf = spark.udf.register("dataQualityUDFForRating", (genres: Array[String]) => {
      if (genres.forall(MOVIE_GENRE_SEQ.contains(_))) "P" else "F"
    })
    udf
  }

  def mapStrToArr(): UserDefinedFunction = {
    val stringToArr = udf((str: String) => str.split("\\|").map(_.trim))
    stringToArr
  }


}
