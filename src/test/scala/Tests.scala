import dev.tomy.Main
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.sql.Date


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

    val expectedData = Seq(
      ("App1", 0.5),
      ("App2", 0.0),
      ("App3", 0.3)
    )

    val expectedDF = expectedData.toDF("App", "Average_Sentiment_Polarity")
    assertDataFrameApproximateEquals(resultDF, expectedDF,"App")

    spark.stop()
  }

  "Part2" should "Obtain Apps with Rating greater or equal to 4.0, sorted in descending order" in {

    val spark = sessionBuilder()

    import spark.implicits._

    val testData = Seq(
      ("App1", 4.0),
      ("App2", 6.0),
      ("App3", 3.9),
      ("App4", 4.5),
      ("App5", 4.2)
    )

    val testDF = testData.toDF("App", "Rating")
    val resultDF = Main.part2(testDF)

    val expectedData = Seq(
      ("App4", 4.5),
      ("App5", 4.2),
      ("App1", 4.0)
    )

    val expectedDF = expectedData.toDF("App", "Rating")


    assertDataFrameEquals(resultDF, expectedDF)
  }

  "Part3" should "Aggregate Categories by App, and take the remaining columns from the App with the most reviews" in {

    val spark = sessionBuilder()

    import spark.implicits._

    val testData = Seq(
      ("App1", "ART", 4.5, "100", "36M","10,000+","Paid","$5.00","Everyone","Art","January 7, 2018","1.0.0","4.0.3 and up" ),
      ("App1", "DESIGN", 4.6, "120", "36.5M","11,000+","Paid","$6.00","Teen","Design","January 8, 2018","1.0.1","4.0.4 and up" ), //Tests if combines genres and takes the values from the one with more reviews
      ("App2", "AUTO", 4.0, "100", "35K","10,000+","Free","0","Everyone","Auto;Vehicles","January 7, 2018","1.0.0","4.0.3 and up" ),


    )

    val testSchema = List("App", "Category", "Rating", "Reviews","Size","Installs","Type","Price","Content Rating","Genres","Last Updated", "Current Ver", "Android Ver")
    val testDF = testData.toDF(testSchema: _*)
    val resultDF = Main.part3(testDF)

    val expectedSchema = List("App", "Categories", "Rating", "Reviews","Size","Installs","Type","Price","Content_Rating","Genres","Last_Updated", "Current_Version", "Minimum_Android_Version")
    val expectedData = Seq(
      ("App1", Array("DESIGN", "ART"), 4.6, 120.toLong, 36.5,"11,000+","Paid",5.4,"Teen",Array("Design"),Date.valueOf("2018-01-08"),"1.0.1","4.0.4 and up" ),
      ("App2", Array("AUTO"), 4.0, 100.toLong, 0.035,"10,000+","Free",0.0,"Everyone",Array("Auto","Vehicles"),Date.valueOf("2018-01-07"),"1.0.0","4.0.3 and up")
    )

    val expectedDF = expectedData.toDF(expectedSchema: _*)

    assertDataFrameApproximateEquals(resultDF,expectedDF,"App")
  }

  "Part4" should "Combine df_3 and df_1 based on column App" in {

    val spark = sessionBuilder()

    import spark.implicits._

    val testData = Seq(
      ("App1", Array("DESIGN", "ART"), 4.6, 120.toLong, 36.5,"11,000+","Paid",5.4,"Teen",Array("Design"),Date.valueOf("2018-01-08"),"1.0.1","4.0.4 and up" ),
      ("App2", Array("AUTO"), 4.0, 100.toLong, 0.035,"10,000+","Free",0.0,"Everyone",Array("Auto","Vehicles"),Date.valueOf("2018-01-07"),"1.0.0","4.0.3 and up")
    )

    val testSchema = List("App", "Category", "Rating", "Reviews","Size","Installs","Type","Price","Content Rating","Genres","Last Updated", "Current Ver", "Android Ver")
    val testDF = testData.toDF(testSchema: _*)

    val testData2= Seq(
      ("App1", 0.5),
      ("App2", 0.3)
    )

    val testSchema2 = List("App", "Average_Sentiment_Polarity")
    val testDF2 = testData2.toDF(testSchema2: _*)
    val resultDF = Main.part4(testDF2,testDF)

    val expectedSchema = List("App", "Categories", "Rating", "Reviews","Size","Installs","Type","Price","Content_Rating","Genres","Last_Updated", "Current_Version", "Minimum_Android_Version","Average_Sentiment_Polarity")
    val expectedData = Seq(
      ("App1", Array("DESIGN", "ART"), 4.6, 120.toLong, 36.5,"11,000+","Paid",5.4,"Teen",Array("Design"),Date.valueOf("2018-01-08"),"1.0.1","4.0.4 and up",0.5),
      ("App2", Array("AUTO"), 4.0, 100.toLong, 0.035,"10,000+","Free",0.0,"Everyone",Array("Auto","Vehicles"),Date.valueOf("2018-01-07"),"1.0.0","4.0.3 and up",0.3)
    )

    val expectedDF = expectedData.toDF(expectedSchema: _*)

    assertDataFrameApproximateEquals(resultDF,expectedDF,"App")
  }

  "Part5" should "Create a new dataframe containing the number of applications, the average rating and the average sentiment polarity by genre" in {

    val spark = sessionBuilder()

    import spark.implicits._

    val testData = Seq(
      ("App1", 4.6, Array("Design")),
      ("App2", 2.2, Array("Design")),
      ("App3", 4.4, Array("Auto","Vehicles")),
      ("App4", 3.6, Array("Vehicles")),
    )

    val testSchema = List("App", "Rating","Genres")
    val testDF = testData.toDF(testSchema: _*)

    val testData2 = Seq(
      ("App1", "0.4"),
      ("App2", "1.0"),
      ("App3", "0.5"),
      ("App3", "0.4"),
      ("App4", "0.3"),
      ("App4", "1.5")
    )

    val testDF2 = testData2.toDF("App", "Sentiment_Polarity")
    val resultDF = Main.part5(testDF, testDF2)

    val expectedSchema = List("Genre", "Count", "Average_Rating","Average_Sentiment_Polarity")
    val expectedData = Seq(
      ("Design",2,3.4,0.7),
      ("Auto",1,4.4,0.45),
      ("Vehicles",2,4.0,0.675)
    )

    val expectedDF = expectedData.toDF(expectedSchema: _*)

    assertDataFrameApproximateEquals(resultDF,expectedDF,"Genre")
  }
  private def assertDataFrameApproximateEquals(actualDF: DataFrame, expectedDF: DataFrame, orderString: String): Unit = {
    assertDataFrameEquals(actualDF.orderBy(orderString), expectedDF.orderBy(orderString))
  }

  private def assertDataFrameEquals(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    assert(actualDF.collectAsList() === expectedDF.collectAsList())
  }


  private def sessionBuilder(): SparkSession = {
    SparkSession.builder().master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").appName("Part_1 Test").getOrCreate()
  }

}