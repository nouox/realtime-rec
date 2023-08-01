package com.fdx.rec.processing.history

import com.fdx.rec.processing.io.{LoadData, SaveModel}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{DataFrame, SparkSession}

object GenOfflineRecModel {
  def main(args: Array[String]): Unit = {

    // 创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("gen_offline_rec_model")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    // 加载历史数据 -> (log, product)
    val oriData: (DataFrame, DataFrame) = LoadData.load(spark)

    // 基于协同过滤的推荐算法
    val cfModel: ALSModel = CollaborativeFilter.process(spark, oriData._1)

    // 基于内容的推荐算法
    val cbModel: ALSModel = ContentBased.process(spark, oriData._1, oriData._2)

    // 深度学习模型

    // 推荐模型落盘
    SaveModel.save(cfModel, cbModel)

    // 关闭sparkSession
    spark.stop()
  }
}