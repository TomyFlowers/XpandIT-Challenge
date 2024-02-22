import dev.tomy.Main
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers


class Tests extends AnyFlatSpec with Matchers {

  "part1" should "convert 'Sentiment_Polarity' column to Double and fill NULL values with 0.0" in {

    val spark = SparkSession.builder().master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").appName("Part_1 Test").getOrCreate()
    // Given
    import spark.implicits._

    val testData = Seq(
      ("App1", "0.3"),
      ("App1", "0.7"),
      ("App2", null),
      ("App3", "0.6"),
      ("App3", null)
    )

    val testDF = testData.toDF("App", "Sentiment_Polarity")
    val resultDF = Main.part1(testDF)

    val expectedSchema = List("App", "Average_Sentiment_Polarity")
    val expectedData = Seq(
      ("App1", 0.5),
      ("App2", 0.0),
      ("App3", 0.3)
    )

    val expectedDF = expectedData.toDF(expectedSchema: _*)

    assertDataFrameApproximateEquals(resultDF, expectedDF)
  }

  private def assertDataFrameApproximateEquals(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    assertDataFrameEquals(actualDF.orderBy("App"), expectedDF.orderBy("App"))
  }

  private def assertDataFrameEquals(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    assert(actualDF.collect() === expectedDF.collect())
  }
}