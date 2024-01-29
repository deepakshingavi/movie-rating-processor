import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

trait SparkSessionTestWrapper extends AnyFunSuite with SharedSparkContext{

//  implicit var spark: SparkSession = _
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    spark = SparkSessionProvider.initSparkSession("local[*]", "Data Quality Test")
//  }
//
//  override def afterAll(): Unit = {
//    super.afterAll()
//    if (spark != null) {
//      spark.stop()
//    }
//  }

}
