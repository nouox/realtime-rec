package com.fdx.rec.utils

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MyConstants {
  val logSchema: StructType = new StructType()
    .add("logId", StringType, nullable = false)
    .add("userId", StringType, nullable = false)
    .add("action", StringType, nullable = false)
    .add("vDate", StringType, nullable = false)
    .add("vTime", StringType, nullable = false)
  val productSchema: StructType = new StructType()
    .add("prdId", StringType, nullable = false)
    .add("title", StringType, nullable = false)
    .add("category", StringType, nullable = false)
    .add("brandId", StringType, nullable = false)
    .add("sellerId", StringType, nullable = false)
  val reviewSchema: StructType = new StructType()
    .add("reviewId", IntegerType, nullable = false)
    .add("userId", StringType, nullable = false)
    .add("feedback", StringType, nullable = false)
    .add("gmtCreate", StringType, nullable = false)

  private val dataFolder: String = "hdfs://hadoop102:9000/rec"

  // 源数据hdfs路径
  val logPath: String = dataFolder + "/log.txt"
  val productPath: String = dataFolder + "/product.txt"
  val reviewPath: String = dataFolder + "/review.txt"

  // 推荐模型hdfs路径
  val models: String = dataFolder + "/model"
  val cfModelPath: String = models + "/cf_model"
  val cbModelPath: String = models + "/cb_model"

  // 用户行为权重
  val clickWeight: Double = 1.0
  val collectWeight: Double = 2.0
  val cartWeight: Double = 3.0
  val alipayWeight: Double = 4.0
}
