package com.fdx.rec.processing.io

import com.fdx.rec.utils.MyConstants
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadData {
  /**
   * 读取hdfs源文件
   *
   * @param spark SparkSession
   * @return
   */
  def load(spark: SparkSession): (DataFrame, DataFrame) = {
    val log: DataFrame = spark.read
      .format("txt")
      .option("sep", "\u0001")
      .option("header", "false")
      .schema(MyConstants.logSchema)
      .load(MyConstants.logPath)

    val product: DataFrame = spark.read
      .format("txt")
      .option("sep", "\u0001")
      .option("header", "false")
      .schema(MyConstants.productSchema)
      .load(MyConstants.productPath)

    (log, product)
  }
}
