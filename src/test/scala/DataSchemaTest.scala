import com.dp.training.model.{Movie, MovieInfo, Rating, User}
import com.dp.training.usecase.BusinessAgg
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.scalatest.funsuite.AnyFunSuite

class DataSchemaTest extends AnyFunSuite with SharedSparkContext
  with DataFrameSuiteBase {

  val baseDir = "src/test/resources/input/"
  override implicit def reuseContextIfPossible: Boolean = true

  test("loadData test for User data") {
    val testDataFilePath = "users.dat"

    implicit val encoder: Encoder[User] = Encoders.product[User]
    // Load test DataFrame using Main.loadData
    val loadedDataFrame: DataFrame = BusinessAgg.loadData[User](baseDir + testDataFilePath)(encoder, spark)

    // Validate count and perform schema check on read as well collect
    assert(!loadedDataFrame.as[User].collectAsList().isEmpty)

  }

  test("loadData test for Rating data") {
    val testDataFilePath = "ratings.dat"


    implicit val encoder: Encoder[Rating] = Encoders.product[Rating]
    // Load test DataFrame using Main.loadData
    val loadedDataFrame: DataFrame = BusinessAgg.loadData[Rating](baseDir +testDataFilePath)(encoder, spark)

    // Validate count and perform schema check on read as well collect
    assert(!loadedDataFrame.as[Rating].collectAsList().isEmpty)

  }

  test("loadData test for Movie data") {
    val testDataFilePath = "movies.dat"

    implicit val encoder: Encoder[Movie] = Encoders.product[Movie]
    // Load test DataFrame using Main.loadData
    val loadedDataFrame: DataFrame = BusinessAgg.loadMovieData[Movie](baseDir +testDataFilePath)(encoder, spark)

    // Validate count and perform schema check on read as well collect
    assert(!loadedDataFrame.as[MovieInfo](Encoders.product[MovieInfo]).collectAsList().isEmpty)

  }

}
