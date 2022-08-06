package org.example

import org.apache.spark.sql.{SparkSession, functions => F}

object ShowAvro {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName("Show DataFrame")
      .getOrCreate();

    var inputPath: String = "gs://capstone-project-wzl-storage/silver/review_logs"
    var format: String = "avro"
    var n_records: Int = 5

    if (args.length == 3){
      inputPath = args(0)
      format = args(1)
      n_records = args(2).toInt
    }

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
}