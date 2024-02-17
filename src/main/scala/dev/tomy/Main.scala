package dev.tomy

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .appName("XpandIT-demo")
      .getOrCreate()

    val df_1 = spark.read
      .option("header", value = true)
      .csv("datasets/googleplaystore.csv")

    df_1.show()
    df_1.printSchema()
    spark.stop()
  }
}
