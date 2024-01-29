import com.dp.training.model.{Movie, MovieInfo, Rating, User}
import com.dp.training.udf.DataQualityUDF
import com.dp.training.usecase.BusinessAgg
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.scalatest.funsuite.AnyFunSuite

class DataQualityTest extends AnyFunSuite with SharedSparkContext
  with DataFrameSuiteBase {

  val baseDir = "src/test/resources/input/"
  override implicit def reuseContextIfPossible: Boolean = true

  test("data quality checks for Rating data") {


    val testDataFilePath = "ratings.dat"

    implicit val encoder: Encoder[Rating] = Encoders.product[Rating]

    val df: DataFrame = BusinessAgg.loadData[Rating](baseDir + testDataFilePath)(encoder, spark)
    val dataQualityUDF = DataQualityUDF.dataQualityCheckForRating(spark);

    val validatedDF = df
      .as[Rating]
      // DataQuality is "P" for valid rows and "F" for invalid rows
      .withColumn("DataQuality", dataQualityUDF(col("UserID"), col("MovieID"), col("Rating")))


    assert(validatedDF.filter(col("DataQuality") === "F").count() == 0)
    assert(validatedDF.filter(col("DataQuality") === "P").count() > 0)
  }

  test("data quality checks for Movie data") {

    val testDataFilePath = "movies.dat"
    implicit val encoder: Encoder[Movie] = Encoders.product[Movie]

    val df: DataFrame = BusinessAgg.loadMovieData[Movie](baseDir + testDataFilePath)(encoder, spark)
    val dataQualityUDF = DataQualityUDF.dataQualityCheckForMovie(spark);

    val validatedDF = df
      .as[MovieInfo](Encoders.product[MovieInfo])
      // DataQuality is "P" for valid rows and "F" for invalid rows
      .withColumn("DataQuality", dataQualityUDF(col("Genres")))


    assert(validatedDF.filter(col("DataQuality") === "F").count() == 0)
    assert(validatedDF.filter(col("DataQuality") === "P").count() > 0)
  }

  test("data quality checks for User data") {

    val testDataFilePath = "users.dat"
    implicit val encoder: Encoder[User] = Encoders.product[User]

    val df: DataFrame = BusinessAgg.loadData[User](baseDir + testDataFilePath)(encoder, spark)
    val dataQualityUDF = DataQualityUDF.dataQualityCheckForUser(spark);

    val validatedDF = df
      .as[User]
      // DataQuality is "P" for valid rows and "F" for invalid rows
      .withColumn("DataQuality", dataQualityUDF(col("Age"), col("Gender"), col("Occupation")))


    assert(validatedDF.filter(col("DataQuality") === "F").count() == 0)
    assert(validatedDF.filter(col("DataQuality") === "P").count() > 0)
  }

}
