package dev.tomy

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("XpandITDemo")
      .getOrCreate()

    println("Printing Spark Session Variables")
    println("App name:" + spark.sparkContext.appName)

    spark.stop()
  }
}
