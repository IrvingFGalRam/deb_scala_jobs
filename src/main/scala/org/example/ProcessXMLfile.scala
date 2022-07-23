package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml

object ProcessXMLfile extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkProcessTest")
    .getOrCreate();

  import spark.implicits._

  println("Reading")
  val df = spark.read.option("header", true).csv("C:/Users/irvin/OneDrive/Documentos/Trabajos/Workshops/Wizeline/CapstoneProject/local/log_reviews.csv")
  println("schema_of_xml")
  val logSchema = schema_of_xml(df.select("log").as[String])
  println("schema_of_xml")
  val parsed = df.withColumn("parsed", from_xml($"log", logSchema))
  println("schema_of_xml")
  val log_rev = parsed.select("id_review", "parsed.log.*")
  println("schema_of_xml")
  log_rev.show(5)
//  log_rev.write.mode("append").parquet("C:/Users/irvin/OneDrive/Documentos/Trabajos/Workshops/Wizeline/CapstoneProject/local/data/log_rev_arch/")
  val log_rev_df = log_rev.select(col("id_review").as("log_id"),
    col("logDate").as("log_date"),
    col("device"),
    col("os"),
    col("location"),
    lit("browser"),
    col("ipAddress").as("ip"),
    col("phoneNumber").as("phone_number")
  )
  log_rev_df.show(5)

}
