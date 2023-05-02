package com.fdx.rec.processing.history

import org.apache.spark.sql.SparkSession

object GenOfflineRecModel {
  def main(args: Array[String]): Unit = {

    // 创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("gen_offline_rec_model")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    // 基于协同过滤的推荐算法
    CollaborativeFilter.process(spark)

    // 基于内容的推荐算法

    // 深度学习模型

    // 关闭sparkSession
    spark.stop()
  }
}