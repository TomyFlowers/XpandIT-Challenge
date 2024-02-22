import dev.tomy.Main
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers


class Tests extends AnyFlatSpec with Matchers {

  "Part1" should "Create new data frame with columns App and Average_Sentiment_Polarity" in {

    val spark = sessionBuilder()

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

    spark.stop()
  }

  "Part2" should "Obtain Apps with Rating greater or equal to 4.0, sorted in descending order" in {

    val spark = sessionBuilder()

    import spark.implicits._

    val testData = Seq(
      ("App1", "4.0"),
      ("App2", "6.0"),
      ("App3", "3.9"),
      ("App4", "4.5"),
      ("App5", "4.2")
    )

    val testDF = testData.toDF("App", "Rating")
    val resultDF = Main.part2(testDF,"test_outputs")

    val expectedSchema = List("App", "Rating")
    val expectedData = Seq(
      ("App4", 4.5),
      ("App5", 4.2),
      ("App1", 4.0)
    )

    val expectedDF = expectedData.toDF(expectedSchema: _*)

    expectedDF.printSchema()
    resultDF.printSchema()

    assertDataFrameEquals(resultDF, expectedDF)
  }

  private def assertDataFrameApproximateEquals(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    assertDataFrameEquals(actualDF.orderBy("App"), expectedDF.orderBy("App"))
  }

  private def assertDataFrameEquals(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    assert(actualDF.collectAsList() === expectedDF.collectAsList())
  }

  private def sessionBuilder(): SparkSession = {
    SparkSession.builder().master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").appName("Part_1 Test").getOrCreate()
  }
}