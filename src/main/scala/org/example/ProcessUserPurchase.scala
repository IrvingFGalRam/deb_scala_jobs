package org.example

import org.apache.spark.sql.functions.{to_timestamp, trim, abs}
import org.apache.spark.sql.types.{IntegerType, StringType, DecimalType}
import org.apache.spark.sql.SparkSession

object ProcessUserPurchase extends App{

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Process User Purchase Job")
    .getOrCreate();

  import spark.implicits._

  val inputCSVpath = sys.env("INPUT_PATH")
  val outputDFpath = sys.env("OUTPUT_PATH")
  val write_format = sys.env("WRITE_FORMAT")


  println("Reading user_purchase_psql file")
  val df = spark.read.option("header", value = true).csv(inputCSVpath)

  // Casting to final DataTypes
  val user_purchase = df.select(
    $"invoice_number".cast(StringType) as "invoice_number",
    trim($"stock_code".cast(StringType)) as "stock_code",
    trim($"detail".cast(StringType)) as "detail",
    abs($"quantity".cast(IntegerType)) as "quantity", // abs to convert negative quantities
    to_timestamp($"invoice_date", "yyyy-MM-dd HH:mm:ss") as "invoice_date",
    abs($"unit_price".cast(DecimalType(8, 3))) as "unit_price", // abs to convert negative prices
    $"customer_id".cast(IntegerType) as "customer_id",
    $"country".cast(StringType) as "country"
  )

  // Data Quality - Filtering
  val data_quality_df = user_purchase.where(
    ($"customer_id".isNotNull) && // Removing null customer_id records
      ($"unit_price" > 0) && // Removes Free item prices
      ($"quantity" > 0) // Removes zero item purchases
  )

  // Saving transformed and cleaned DF
  data_quality_df.write
    .mode("overwrite")
    .partitionBy("country")
    .format(write_format)
    .save(outputDFpath)
}
