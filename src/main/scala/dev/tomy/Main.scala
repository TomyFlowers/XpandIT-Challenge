package dev.tomy

import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array_distinct, avg, coalesce, col, collect_list, collect_set, desc, first, lit, max, rank, regexp_replace, row_number, to_date, when}


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

/*
    //Part 1
    //Converts "Sentiment_Polarity" column from String to Double and transforms NULL to 0
    val castedUserReviews= userReviews.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double")).na.fill(0.0)
    val df_1 = castedUserReviews.groupBy("App").agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))

    //Part 2
    val df_2 = playStoreApps
      .withColumn("Rating", coalesce(col("Rating"), lit(0)))
      .filter(col("Rating").between(4,5)) //Excludes ratings above 5
      .repartition(3) //Error caused by sorting without repartitioning
      .orderBy(col("Rating").desc)

    Utils.saveCSV(df_2,"outputs","best_apps",",")
*/
    //Part 3
    val windowSpec = Window.partitionBy("App").orderBy(col("Reviews").desc)

    val df_3 = playStoreApps
      .withColumn("Rank", rank().over(windowSpec))
      .orderBy("Rank")
      .groupBy("App")
      .agg(
        array_distinct(collect_list("Category")).alias("Categories"),
        first("Rating").as("Rating"),
        max(coalesce(col("Reviews"),lit(0))).cast("long").as("Reviews"),
        when(first("Size").endsWith("M"), regexp_replace(first("Size"), "[^\\d.]+", "").cast("double"))
          .when(first("Size").endsWith("k"), regexp_replace(first("Size"), "[^\\d.]+", "").cast("double") * 0.001)
          .otherwise(null)
          .as("Size"),
        first("Installs").as("Installs"),
        first("Type").as("Type"),
        //first(regexp_replace(col("Price"), "[^\\d.]+", "").cast("double")).as("Price"),
        (regexp_replace(first("Price"), "[^\\d.]+", "").cast("double")*0.9).as("Price"),
        first("Content Rating").as("Content_Rating"),
        first("Genres").as("Genres"),
        to_date(first("Last Updated"), "MMM dd, yyyy").as("Last_Updated"),
        first("Current Ver").as("Current_Version"),
        first("Android Ver").as("Minimum_Android_Version")
      )

    sparkSession.stop()
  }
}
