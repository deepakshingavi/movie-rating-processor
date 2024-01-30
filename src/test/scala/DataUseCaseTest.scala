import com.dp.training.usecase.BusinessAgg
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths


class DataUseCaseTest extends AnyFunSuite with SharedSparkContext
  with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true


  val outputDir = Paths.get("src/test/resources/output/")
  override def beforeAll(): Unit = {

    super.beforeAll()

  }

  override def afterAll(): Unit = {
    super.beforeAll()

  }



  test(" test max,min and avg rating calculation logic ") {

    val data = Seq(
      (1, 4),
      (2, 3),
      (1, 5),
      (2, 2),
      (3, 4)
    )

    val columns = Seq("MovieID", "Rating")
    val df = spark.createDataFrame(data).toDF(columns: _*)

    val resultDF = BusinessAgg.ratingAgg(df)

    val expectedResults = Seq(
      (1, 5, 4, 4.5),
      (2, 3, 2, 2.5),
      (3, 4, 4, 4.0)
    )

    val expectedColumns = Seq("MovieID", "MaxRating", "MinRating", "AvgRating")
    val expectedDF = spark.createDataFrame(expectedResults).toDF(expectedColumns: _*)

    assertDataFrameDataEquals(resultDF, expectedDF)

  }


  test("getLatestTop3RateMoviesForUser should return the latest top 3 rated movies for each user") {

    // Sample DataFrame (replace this with your actual DataFrame)
    val data = Seq(
      (1, 101, 4.5, 1000),
      (1, 102, 3.5, 1100),
      (1, 103, 5.0, 1200),
      (2, 201, 4.0, 900),
      (2, 202, 3.0, 800),
      (2, 203, 3.5, 950),
      (3, 301, 2.0, 1300),
      (3, 302, 2.5, 1400),
      (3, 303, 1.5, 1250)
    )

    val columns = Seq("UserID", "MovieID", "Rating", "Timestamp")
    val df = spark.createDataFrame(data).toDF(columns: _*)

    // Call the getLatestTop3RateMoviesForUser method
    val resultDF = BusinessAgg.getLatestTop3RateMoviesForUser(df)

    // Validate the results
    val expectedResults = Seq(
      (1, 103, 5.0, 1200),
      (1, 101, 4.5, 1000),
      (1, 102, 3.5, 1100),
      (2, 201, 4.0, 900),
      (2, 203, 3.5, 950),
      (2, 202, 3.0, 800),
      (3, 302, 2.5, 1400),
      (3, 301, 2.0, 1300),
      (3, 303, 1.5, 1250)
    )

    val expectedColumns = Seq("UserID", "MovieID", "Rating", "Timestamp")
    val expectedDF = spark.createDataFrame(expectedResults).toDF(expectedColumns: _*)

//    compareDataFrameEquals(resultDF, expectedDF)
    assertDataFrameDataEquals(resultDF, expectedDF)
  }


  test("writeMovieData method should write data successfully") {

    import spark.implicits._

    // Create a test DataFrame
    val testData = Seq((1, "Movie1"), (2, "Movie2")).toDF("MovieID", "MovieName")

      // Specify test output path and table name
//      val outputFilePath = "src/test/resources/output/"
      val tableName = "movie_data"

      // Call the writeMovieData method
      BusinessAgg.writeMovieData(testData, outputDir.toAbsolutePath.toString, tableName)

      // Load the written data and assert

      val loadedData = spark.read.parquet(outputDir.toAbsolutePath.toString  + tableName)
      assertDataFrameDataEquals(testData, loadedData)

  }

  test("writeRatingData method should write data successfully") {

    import spark.implicits._
    val testData = Seq(
      (1, 101, 5, 1234567890),
      (2, 102, 4, 1234567891),
      (3, 103, 3, 1234567892)
    ).toDF("RatingID", "MovieID", "UserID", "Timestamp")

    // Specify test output path and table name

    val tableName = "rating_data"

    // Call the writeRatingData method
    BusinessAgg.writeRatingData(testData, outputDir.toAbsolutePath.toString, tableName)

    // Load the written data and assert
    val loadedData = spark.read.parquet(outputDir.toAbsolutePath.toString  + tableName)

    assert(loadedData.columns.contains("Year"))

    assert(loadedData.count() == testData.count())
    // Additional assertions based on your specific use case
    assert(loadedData.select("Year").distinct().count() == 1) // Expecting one distinct year
  }

}
