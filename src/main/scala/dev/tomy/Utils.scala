package dev.tomy

import org.apache.spark.sql.{DataFrame, SparkSession}


object Utils {

  def loadCSV(session: SparkSession,pathToCSV: String): DataFrame = {
    val reviews = session.read
      .option("header", value = true)
      /*Double quotes is an escape character and is used for quoting inside quoted value.
       The option bellow allows it to be treated as regular character */
      .option("escape","\"")
      .csv("datasets/googleplaystore_user_reviews.csv")

    reviews
  }



}