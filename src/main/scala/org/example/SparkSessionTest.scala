package org.example
import org.apache.spark.sql.SparkSession
object SparkSessionTest extends App{
//  Followed guide: https://sparkbyexamples.com/spark/spark-setup-run-with-scala-intellij/

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  // Testing program Arguments
  println("\t\tProgram Arguments:")
  println(args.mkString(", "))

  // Testing Environment Variables
  println("ENV" + sys.env("TEST_ENV"))

  println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);

  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample-test")
    .getOrCreate();

  println("Second SparkContext:")
  println("APP Name :"+sparkSession2.sparkContext.appName);
  println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
  println("Master :"+sparkSession2.sparkContext.master);
}