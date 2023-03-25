package com.fdx.rec.utils

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MyConstants {

  val logSchema: StructType = new StructType()
    .add("itemId", IntegerType, nullable = false)
    .add("userId", StringType, nullable = false)
    .add("action", StringType, nullable = false)
    .add("vTime", StringType, nullable = false)
  val productSchema: StructType = new StructType()
    .add("title", StringType, nullable = false)
    .add("category", StringType, nullable = false)
    .add("brandId", StringType, nullable = false)
    .add("sellerId", StringType, nullable = false)
  val reviewSchema: StructType = new StructType()
    .add("itemId", IntegerType, nullable = false)
    .add("userId", StringType, nullable = false)
    .add("feedback", StringType, nullable = false)
    .add("gmtCreate", StringType, nullable = false)

  val dataFolderPath: String = "hdfs://hadoop102:9000/..."
  val logCsvPath = dataFolderPath + "/log.csv"
  val productCsvPath = dataFolderPath + "/product.csv"
  val reviewCsvPath = dataFolderPath + "/review.csv"

}
