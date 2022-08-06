package org.example

import org.apache.spark.sql.{SparkSession, functions => F}

object LocalShowAvro extends App{

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Show Dataframe")
    .getOrCreate();

  val inputPath = sys.env("INPUT_PATH")
  val format = sys.env("INPUT_FORMAT")
  val n_records = sys.env("N_RECORDS").toInt

  val df = spark.read.format(format).load(inputPath)

  println("DataFrame Schema")
  df.printSchema

  println("DataFrame Show")
  df.show(n_records, truncate = false)

  val nulls_ls: Array[String] =
    for (col <- df.columns)
      yield col + "\t\t" + df.filter(F.col(col).isNull).count
  println("DF Count of null records")
  nulls_ls.foreach(println)
}
