import org.apache.spark.sql.functions._

object Test {
  def main(args: Array[String]): Unit = {
//    val user_data_schema = new StructType(Array(
//      StructField("UserID", StringType, true),
//      StructField("Gender", StringType, true),
//      StructField("Age", StringType, true),
//      StructField("Occupation", StringType, true),
//      StructField("ZipCode", StringType, true)
//    ))
//
//    val user_df = spark.read.option("delimiter","::").schema(user_data_schema).csv("/Users/dshingav/openSourceCode/movie-rating-processor/src/main/resources/users.dat")
//
//    val movies_schema = new StructType(
//      Array(
//        StructField("MovieID", StringType, true),
//        StructField("Title", StringType, true),
//        StructField("Genres", StringType, true)
//      )
//    )
//    val stringToArr = udf((str: String) => str.split(",").map(_.trim))
//
//    val movies_df = spark.read.option("delimiter","::").schema(movies_schema).csv("/Users/dshingav/Downloads/ml-1m/movies.dat").withColumn("Generes", stringToArr(col("Genres")))
//
//    val ratings_schema = new StructType(
//      Array(
//        StructField("UserID", StringType, true),
//        StructField("MovieID", StringType, true),
//        StructField("Rating", StringType, true),
//        StructField("Timestamp", LongType, true)
//      )
//    )
//
//    val ratings_df = spark.read.option("delimiter","::").schema(ratings_schema).csv("/Users/dshingav/openSourceCode/movie-rating-processor/src/main/resources/ratings.dat")
//
//
//    val movie_ratings_df = ratings_df.groupBy("MovieID").agg(
//      max("Rating").alias("MaxRating"),
//        min("Rating").alias("MinRating"),
//    avg("Rating").alias("AvgRating")
//    )
//
//    val partitionByUserOrderByRating = Window.partitionBy("UserID").orderBy(desc("Rating"),desc("Timestamp"))
//
//    val topRatedMovieForThatUser = ratings_df.withColumn("RowNum", row_number().over(partitionByUserOrderByRating)).filter("RowNum <= 3").drop("RowNum")
//
//    topRatedMovieForThatUser
//      .show()

  }

}