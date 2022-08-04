package org.example

import org.apache.spark.ml.feature.{NGram, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{Column, SparkSession, functions => F}

import scala.collection.mutable.ListBuffer

object TransformClassifiedMovieReview {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Classified Movie Review Job")
      .getOrCreate();

    import spark.implicits._

    val inputCSVpath = "gs://capstone-project-wzl-storage/bronze/movie_review.csv"
    val outputDFpath = "gs://capstone-project-wzl-storage/silver/classified_movie_review"

    println("Reading CSV")
    val df_movie_review = spark.read.option("header", value = true).csv(inputCSVpath)

    // Cleaning review string
    val chars_to_remove = ".,;-_()'!¡*"
    val strings_to_remove = "<br />"
    val df = df_movie_review.withColumn("clean_review", F.translate($"review_str", chars_to_remove, "")
    ).withColumn("clean_review", F.regexp_replace($"clean_review", strings_to_remove, ""))

    // Configuring tokenizer to split review into words and remover to filter out non contextual words
    val tokenizer = new Tokenizer().setInputCol("clean_review").setOutputCol("words")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")

    // Remove key words from stopWords. To disable the words from being filtered out
    val ok_words = List("isn't", "its", "wasn't", "couldn't", "above")

    //  var stopWords = remover.getStopWords.to(ListBuffer)
    var stopWords = remover.getStopWords.to(ListBuffer)
    ok_words.foreach(stopWords -= _)
    val cstm_stopWords = for (s <- stopWords)
      yield s.replace("\'", "")

    // Applying separation and filtering
    val tokenized_df = remover.transform(tokenizer.transform(df))

    // Grouping each 2 words to try and get a little bit more context
    val ngram = new NGram().setN(2).setInputCol("filtered").setOutputCol("ngrams")
    val ngram_df = ngram.transform(tokenized_df)

    // Setting individual words and word combinations to assign positive_review
    val good_ls = List("good", "great", "cool", "neat", "best", "solid", "average", "genius", "incredible")
    val ratings_ls = List("5/10", "6/10", "7/10", "8/10", "9/10", "10/10")
    val good_simple_ls = good_ls ++ ratings_ls

    val aux_ls = List("movie", "film", "its", "great", "frankly", "just", "plain", "pretty")

    val bad_ls = List("bad", "horrible", "mediocre", "boring", "worst", "sucks", "stinks", "dissapointment")
    val modifier_ls = List("wasnt", "isnt", "not")

    var good_complex_ls = ListBuffer("couldnt better", "above average", "master piece", "half bad")
    var bad_complex_ls = ListBuffer[String]()
    // Setting up the good "complex" two-word search terms
    for (bad <- bad_ls) {
      good_complex_ls ++= (
        for (mod <- modifier_ls) yield (mod + " " + bad)
        )
    }
    for (good <- good_ls) {
      good_complex_ls ++= (
        for (aux <- aux_ls) yield (aux + " " + good)
        )
    }
    for (good <- good_ls) {
      good_complex_ls ++= (
        for (aux <- aux_ls) yield (good + " " + aux)
        )
    }
    // List so it can be unpacked https://stackoverflow.com/questions/15034565/is-there-a-scala-equivalent-of-the-python-list-unpack-a-k-a-operator
    val good_search_terms: List[Column] = (for (search_term <- good_complex_ls) yield F.lit(search_term)).toList
    // Setting up the bad "complex" two-word search terms
    for (good <- good_ls) {
      bad_complex_ls ++= (
        for (mod <- modifier_ls) yield (mod + " " + good)
        )
    }
    for (bad <- bad_ls) {
      bad_complex_ls ++= (
        for (aux <- aux_ls) yield (aux + " " + bad)
        )
    }
    for (bad <- bad_ls) {
      bad_complex_ls ++= (
        for (aux <- aux_ls) yield (bad + " " + aux)
        )
    }
    val bad_search_terms: List[Column] = (for (search_term <- bad_complex_ls) yield F.lit(search_term)).toList

    // Searching for simple one-word occurrences
    var final_df = ngram_df.withColumn("is_good",
      F.when(F.array_contains(ngram_df("filtered"), "good"), "1")
        .otherwise("0")
    )
    final_df = final_df.withColumn("is_bad",
      F.when(F.array_contains(final_df("filtered"), "bad"), "1")
        .otherwise("0")
    )
    // Searching for "complex" two-word occurrences
    final_df = final_df.withColumn("complex_good",
      F.when(
        F.size(
          F.array_intersect(final_df("ngrams"), F.array(good_search_terms: _*))
        )
          > 0,
        "1"
      ).otherwise("0")
    )
    final_df = final_df.withColumn("complex_bad",
      F.when(
        F.size(
          F.array_intersect(final_df("ngrams"), F.array(bad_search_terms: _*))
        )
          > 0,
        "1"
      ).otherwise("0")
    )

    // Defining logic to stablish positive review, and including a timestamp
    val movie_review_df =
      final_df.withColumn("positive_review",
        F.when(
          ((F.col("is_good") === "1") && (F.col("complex_bad") === "0")
            ) ||
            ((F.col("complex_good") === "1") && (F.col("complex_bad") === "0")),
          1
        ).otherwise(0)
      ).withColumn("insert_date",
        F.current_timestamp()
      )

    // Saving as Avro, partitioning by positive_review to improve further processing
    movie_review_df.select(
      F.col("cid").alias("user_id"),
      $"positive_review",
      F.col("id_review").alias("review_id"),
      $"insert_date"
    ).write
      .mode("overwrite")
      .partitionBy("positive_review")
      .format("avro")
      .save(outputDFpath)
  }
}