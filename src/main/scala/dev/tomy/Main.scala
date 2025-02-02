package dev.tomy

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, coalesce, col, collect_set, countDistinct, explode, first, lit, max, rank, regexp_replace, split, to_date, when}


object Main {
  def main(args: Array[String]): Unit = {

    // Load config
    val properties = new Properties()
    val inputStream = getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(inputStream)

    //Creates Spark Session
    val sparkSession = SparkSession.builder()
      .master(properties.getProperty("spark.master"))
      .config("spark.driver.bindAddress", properties.getProperty("spark.bindAddress"))
      .appName(properties.getProperty("spark.appName"))
      .getOrCreate()

    //Load csv files
    val userReviews = Utils.loadCSV(sparkSession, properties.getProperty("app.userReviewsPath"))
    val playStoreApps = Utils.loadCSV(sparkSession, properties.getProperty("app.playStoreAppsPath"))

    val outputPath = properties.getProperty("app.outputPath")

    val df_1 = part1(userReviews)
    val df_2 = part2(playStoreApps)
    Utils.saveCSV(df_2,outputPath,"best_apps","$")
    val df_3 = part3(playStoreApps)
    val joinedDF = part4(df_1, df_3)
    Utils.saveParquet(joinedDF, outputPath, "googleplaystore_cleaned")
    val df_4 = part5(df_3, userReviews)
    Utils.saveParquet(df_4, outputPath, "googleplaystore_metrics")

    sparkSession.stop()
  }

   def part1(userReviews: DataFrame): DataFrame = {
    //Converts "Sentiment_Polarity" column from String to Double and transforms NULL to 0
    val castedUserReviews = userReviews.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double")).na.fill(0.0)
    castedUserReviews.groupBy("App").agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))
  }

  def part2(playStoreApps: DataFrame): DataFrame = {
     playStoreApps
      .withColumn("Rating", coalesce(col("Rating"), lit(0)))
      .filter(col("Rating").between(4,5)) //Excludes ratings bellow 4 and above 5
      .repartition() //Error caused by sorting without repartitioning
      .orderBy(col("Rating").desc)
  }

  def part3(playStoreApps: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("App").orderBy(col("Reviews").desc)

    val df_3 = playStoreApps
      .withColumn("Rank", rank().over(windowSpec))
      .orderBy("Rank") //Not the simplest way to sort. The same result could be achieved simply by sorting for "Reviews", but "orderBy" was causing errors
      .groupBy("App")
      .agg(
        collect_set("Category").alias("Categories"), //array containing all unique "Category" column values
        first("Rating").as("Rating"),
        max(coalesce(col("Reviews"), lit(0))).cast("long").as("Reviews"),
        when(first("Size").endsWith("M"), regexp_replace(first("Size"), "[^\\d.]+", "").cast("double")) //regex replaces non-numeric characters with an empty string
          .when(first("Size").endsWith("K"), regexp_replace(first("Size"), "[^\\d.]+", "").cast("double") * 0.001)
          .otherwise(null)
          .as("Size"),
        first("Installs").as("Installs"),
        first("Type").as("Type"),
        (regexp_replace(first("Price"), "[^\\d.]+", "").cast("double") * 0.9).as("Price"),
        first("Content Rating").as("Content_Rating"),
        split(first("Genres"), ";").cast("array<string>").as("Genres"),
        to_date(first("Last Updated"), "MMM dd, yyyy").as("Last_Updated"),
        first("Current Ver").as("Current_Version"),
        first("Android Ver").as("Minimum_Android_Version")
      )
    df_3
  }

  def part4(df_1: DataFrame, df_3: DataFrame): DataFrame = {
    df_3.join(df_1, "App")
  }

  def part5(df_3: DataFrame, userReviews: DataFrame): DataFrame = {
    val explodedDf_3 = df_3.select(
      col("App"), explode(col("Genres")).alias("Genre"),
      col("Rating"))

     explodedDf_3.join(userReviews, "App")
      .groupBy("Genre")
      .agg(
        countDistinct("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
  }
}
