package dev.tomy

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File


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
    df.coalesce(1)
      .write
      .option("header", value = true)
      .option("escape","\"")
      .mode("overwrite")
      .option("delimiter", delimiter)
      .csv(s"$outputPath/temp_$fileName") //saves to temp dir
    cleanup(outputPath,fileName,"csv")
  }

  def saveParquet(df: DataFrame, outputPath: String, fileName: String): Unit = {
    df.coalesce(1)
      .write
      .option("header", value = true)
      .option("escape","\"")
      .option("compression","gzip")
      .mode("overwrite")
      .parquet(s"$outputPath/temp_$fileName") //saves to temp dir
    cleanup(outputPath,fileName,"parquet")
  }

  private def cleanup(outputPath: String, fileName: String, fileType: String): Unit = {
    val tempDir = new File(s"$outputPath/temp_$fileName")

    if (tempDir.exists()) {
      val generatedFiles = tempDir.listFiles() //Gets all files from temp dir
      val mainFile = generatedFiles.find(_.getName.endsWith(fileType)) //gets the main file based on provided file type. Either csv or parquet

      mainFile.get.renameTo(new File(s"$outputPath/$fileName.$fileType")) //Renames the file and moves it to final output dir

      // Remove temporary files and directory
      generatedFiles.foreach(_.delete())
      tempDir.delete()
    }
  }
}