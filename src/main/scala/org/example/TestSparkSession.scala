package org.example

import org.apache.spark.sql.SparkSession

object TestSparkSession {

  def main(args: Array[String]) {

    if (args.length == 2) {
      // Testing program Arguments
      println("\t\tProgram Arguments:")
      val inputPath = args(0)
      val outputPath = args(1)
      println(inputPath)
      println(outputPath)
    }


    val spark = SparkSession.builder()
      .appName("TestSparkSessionJob")
      .getOrCreate();



    println("\t\tCount")
    val df = spark.read.option("header", value = true).csv("gs://capstone-project-wzl-storage/bronze/log_reviews.csv")
    println(df.count())
    println("\t\tEnd Count")


    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

    val sparkSession2 = SparkSession.builder()
      .appName("SparkByExample-test")
      .getOrCreate();

    println("Second SparkContext:")
    println("APP Name :" + sparkSession2.sparkContext.appName);
    println("Deploy Mode :" + sparkSession2.sparkContext.deployMode);
    println("Master :" + sparkSession2.sparkContext.master);
  }
}