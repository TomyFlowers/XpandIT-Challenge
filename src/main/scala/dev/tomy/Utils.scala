package dev.tomy

import org.apache.spark.sql.{DataFrame, SparkSession}


object Utils {

  def loadCSV(session: SparkSession,pathToCSV: String): DataFrame = {
    val reviews = session.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("escape","\"") //Double quotes is an escape character and is used for quoting inside quoted value.This option allows it to be treated as regular character
      .csv(pathToCSV)

    reviews
  }

  def saveCSV(df: DataFrame,outputPath: String, fileName: String, delimiter: String): Unit = {
    df.write
      .option("header", value = true)
      .option("escape","\"")
      .mode("overwrite")
      .option("delimiter", delimiter)
      .csv(s"$outputPath/$fileName")
  }

  def deleteExcessFiles(): Unit = {

  }



}