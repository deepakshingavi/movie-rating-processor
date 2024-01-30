import com.dp.training.model.{Movie, Rating, User}
import com.dp.training.usecase.BusinessAgg
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.funsuite.AnyFunSuite


class DataUseCaseTest extends AnyFunSuite with SharedSparkContext
  with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true


  val outputDir = "src/test/resources/output/"
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
    import spark.implicits._

    // Sample DataFrame (replace this with your actual DataFrame)
    val rating_logs = Seq(
      Rating(1, 101, 4.5, 1000),
      Rating(1, 102, 3.5, 1100),
      Rating(1, 103, 5.0, 1200),
      Rating(1, 104, 3.5, 1201),
      Rating(2, 201, 4.0, 900),
      Rating(2, 202, 3.0, 800),
      Rating(2, 203, 3.5, 950),
      Rating(2, 230, 5, 950),
      Rating(3, 301, 2.0, 1300),
      Rating(3, 302, 2.5, 1400),
      Rating(3, 303, 1.5, 1250),
      Rating(3, 303, 1.5, 4500)
    ).toDF()

    // Call the getLatestTop3RateMoviesForUser method
    val resultDF  = BusinessAgg.getLatestTop3RateMoviesForUser(rating_logs)

    // Validate the results
    val expectedDF  = Seq(
      Rating(1, 103, 5.0, 1200),
      Rating(1, 101, 4.5, 1000),
      Rating(1, 104, 3.5, 1201),
      Rating(2, 230, 5, 950),
      Rating(2, 201, 4.0, 900),
      Rating(2, 203, 3.5, 950),
      Rating(3, 302, 2.5, 1400),
      Rating(3, 301, 2.0, 1300),
      Rating(3, 303, 1.5, 4500)
    ).toDF()


    assertDataFrameDataEquals(resultDF, expectedDF)
  }


  test("writeMovieData method should write data successfully") {

    import spark.implicits._

    // Create a test DataFrame
    val movieData = Seq(
      Movie(1, "Movie1", "Action"),
      Movie(2, "Movie2", "Drama"),
      // Add more Movie instances as needed
    ).toDF()

      // Specify test output path and table name
//      val outputFilePath = "src/test/resources/output/"
      val tableName = "movie_data"

      // Call the writeMovieData method
      BusinessAgg.writeMovieData(movieData, outputDir, tableName)

      // Load the written data and assert

      val loadedData = spark.read.parquet(outputDir  + tableName)
      assertDataFrameDataEquals(movieData, loadedData)

  }

  test("writeUserData method should write data successfully") {

    import spark.implicits._

    // Create a test DataFrame
    val userData = Seq(
      User(1, "Male", "25-34", 4, "12345"),
      User(2, "Female", "18-24", 7, "56789")
    ).toDF()

    // Specify test output path and table name
    //      val outputFilePath = "src/test/resources/output/"
    val tableName = "user_data"

    // Call the writeMovieData method
    BusinessAgg.writeUserData(userData, outputDir, tableName)

    // Load the written data and assert

    val loadedData = spark.read.parquet(outputDir  + tableName)
    assertDataFrameDataEquals(userData, loadedData)

  }

  test("writeRatingData method should write data successfully") {

    import spark.implicits._
    val ratingData = Seq(
      Rating(1, 101, 5, 1234567890),
      Rating(2, 102, 4, 1234567891),
      Rating(3, 103, 3, 1234567892)
    ).toDF()

    val tableName = "rating_data"
    BusinessAgg.writeRatingData(ratingData, outputDir, tableName)


    val loadedData = spark.read.parquet(outputDir + tableName)

    assert(loadedData.columns.contains("Year"))

    assert(loadedData.count() == ratingData.count())

    assert(loadedData.select("Year").distinct().count() == 1) // Expecting one distinct year
  }

}
