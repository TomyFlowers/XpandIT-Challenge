package dev.tomy

import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, coalesce, col, lit}


object Main {
  def main(args: Array[String]): Unit = {

    // Load config
    val properties = new Properties()
    val inputStream = getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(inputStream)

    //Creates Spark Session
    val session = SparkSession.builder()
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .appName("XpandIT-demo")
      .getOrCreate()


    //Part 1
    val userReviews = Utils.loadCSV(session,properties.getProperty("app.userReviewsPath"))
    userReviews.show()
    //Converts "Sentiment_Polarity" column from String to Double and transforms NULL to 0
    val castedUserReviews= userReviews.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double")).na.fill(0.0)
    val df_1 = castedUserReviews.groupBy("App").agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))

    session.stop()
  }
}
