package org.example
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import org.apache.spark.sql.types.{StringType, StructType}

object ProcessReviewLogs extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Process Review Logs Job")
    .getOrCreate();

  import spark.implicits._

  val inputCSVpath = sys.env("INPUT_PATH")
  val outputDF = sys.env("OUTPUT_PATH")

  println("Reading log_reviews file")
  val df = spark.read.option("header", value = true).csv(inputCSVpath)
  // Parses the schema from XML at "log" column
  val logSchema = schema_of_xml(df.select("log").as[String])
  // Inserts "parsed" column with the "log" xml content as a Struct col
  val parsed = df.withColumn("parsed", from_xml($"log", logSchema))
  // Unpaking DF parsed.log columns
  val log_rev = parsed.select("id_review", "parsed.log.*")
  log_rev.show(5)

  // Auxiliary DF to fill browsers
  val device_browserData = Seq(
    Row("Microsoft Windows", "Edge"),
    Row("Linux", "Firefox"),
    Row("Apple iOS", "Safari"),
    Row("Apple MacOS", "Safari"),
    Row("Google Android", "Chrome")
  )
  val device_browserSchema = new StructType()
    .add("os_aux",StringType)
    .add("browser",StringType)
  val device_browser = spark.createDataFrame(spark.sparkContext.parallelize(device_browserData), device_browserSchema)
  device_browser.show()

  // Join to set browser according to the device's OS
  val log_rev_tmp = log_rev.join(device_browser,
    log_rev("os") === device_browser("os_aux"),
    "left"
  )
  log_rev_tmp.show()

  // Renaming cols and inserting browser column
  val log_rev_df = log_rev_tmp.select($"id_review" as "log_id",
    $"logDate" as "log_date",
    $"device",
    $"os",
    $"location",
    $"browser",
    $"ipAddress" as "ip",
    $"phoneNumber" as "phone_number"
  )
  log_rev_df.show(5)

  // Saving transformed DF as Avro (row based, better when dealing with the whole table)
  log_rev_df.write.mode("overwrite").format("avro").save(outputDF)
}
