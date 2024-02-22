# XpandIT-Challenge
This repository contains my solution to [XpandIT's Spark 2 Recruitment Challenge](https://github.com/bdu-xpand-it/BDU-Recruitment-Challenges/wiki/Spark-2-Recruitment-Challenge).
This program processes 2 .csv files and produces 3 files.
The repository already contains the produced files in the outputs folder. That can be obtained by 
running the program in it's default configuration.

## Configuration
The project can be configured through the config.properties file,
that allows the configuration of the SparkSession and the configuration
of some Application Parameters, like the input files location and the output folder.

## Running
The program can be run by importing it into an IDE as a Maven project, and creating a 
run configuration in the main class.   

## Project Structure
- **scr/main/scala**: Source code and configuration file
- **src/test/scala**: Test code
- **pom.xml:** Maven project configuration file
## Dependencies
The dependencies in this project are managed by maven and are listed in the `pom.xml` file.

- Scala - Version 
- ScalaTest - Version 3.2.18
- Apache Spark - 2.12.18

## Notes
One of the rows in the `gogoleplaystore.csv` file was missing the
category column, but for the sake of simplicity it was left unchanged.

