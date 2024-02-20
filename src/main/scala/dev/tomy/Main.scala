package dev.tomy

import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, coalesce, col, desc, lit}


object Main {
  def main(args: Array[String]): Unit = {

    // Load config
    val properties = new Properties()
    val inputStream = getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(inputStream)

    //Creates Spark Session
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .appName("XpandIT-demo")
      .getOrCreate()

    val userReviews = Utils.loadCSV(sparkSession,properties.getProperty("app.userReviewsPath"))
    val playStoreApps = Utils.loadCSV(sparkSession,properties.getProperty("app.playStoreAppsPath"))

    //Part 1
    //Converts "Sentiment_Polarity" column from String to Double and transforms NULL to 0
    ////val castedUserReviews= userReviews.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double")).na.fill(0.0)
    ////val df_1 = castedUserReviews.groupBy("App").agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))

    //Part 2

    val filteredApps = playStoreApps.withColumn("Rating", coalesce(col("Rating"), lit(0)))
      .filter(col("Rating") >= 4)
      .filter(col("Rating") <= 5)

    Utils.saveCSV(filteredApps,"outputs","df_2",",")

    sparkSession.stop()
  }
}
