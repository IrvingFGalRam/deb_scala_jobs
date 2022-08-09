package org.example

import org.apache.spark.sql.{SparkSession, functions => F}

object TransformMovieAnalytics {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName("Movie Analytics Job")
      .getOrCreate();

    import spark.implicits._

    val inputCMRpath: String = "gs://capstone-project-wzl-storage/silver/classified_movie_review"
    val inputRLpath: String = "gs://capstone-project-wzl-storage/silver/review_logs"
    val inputUPpath: String = "gs://capstone-project-wzl-storage/silver/user_purchase"
    val outputDFpath: String = "gs://capstone-project-wzl-storage/gold/movie_analytics"
    var read_format: String = "avro"

    if (args.length == 1){
      read_format = args(0)
    }

    println("Reading tables")
    val cmr_df = spark.read.format(read_format).load(inputCMRpath)
    val rl_df = spark.read.format(read_format).load(inputRLpath)
    val up_df = spark.read.format(read_format).load(inputUPpath)

    // Aggregating customer purchases
    val agg_up_df =
      up_df.groupBy("customer_id")
        .agg(
          F.sum(up_df("quantity") * up_df("unit_price")).alias("amount_spent")
        )

    // Inner joining reviews, so only complete reviews are preserved
    val rl_cols = rl_df.columns.toSeq
    val join_reviews_df =
      cmr_df.join(
        rl_df,
        cmr_df("review_id") === rl_df("log_id"),
        "inner"
      ).select(
        $"user_id",
        $"review_id",
        F.struct(rl_cols.head, rl_cols.tail: _*) as "review_log", // Packing review logs as struct
        $"insert_date",
        $"positive_review"
      )
    // Aggregating reviews over user/customers to obtain counts
    val agg_reviews_df =
      join_reviews_df.groupBy("user_id")
        .agg(
          F.sum("positive_review") as "review_score",
          F.count("review_id") as "review_count",
          F.collect_list("review_log") as "review_info",
          F.first("insert_date") as "insert_date"
        )

    // Left Joining in order to preserve the bast amount of customer records
    val movie_analytics_df =
      agg_up_df.join(
        agg_reviews_df,
        agg_up_df("customer_id") === agg_reviews_df("user_id"),
        "left"
      ).drop("user_id")

    // Writing transformed and cleaned DF
    println("Writing movie_analytics")
    movie_analytics_df.write
      .mode("overwrite")
      .format("parquet")
      .save(outputDFpath)
  }
}