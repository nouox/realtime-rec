package com.fdx.rec.processing.history

import com.fdx.rec.utils.MyConstants
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 协同过滤算法是根据用户历史行为数据推荐相似用户喜欢的物品。
 * 基于用户行为历史，构建用户-商品矩阵
 * 最后使用协同过滤算法来推荐相似的商品
 */
object CollaborativeFilter {
  def process(spark: SparkSession): Unit = {
    // 读取log数据
    val log: DataFrame = spark.read
      .format("txt")
      .option("sep", "\u0001")
      .option("header", "false")
      .schema(MyConstants.logSchema)
      .load(MyConstants.logPath)

    // 数据预处理
    // action操作转换为评分数值
    val ratingLog: DataFrame = log.select("user_id", "item_id", "action")
      .withColumn("rating", when(
        col("action") === "pay", 1.0).otherwise(0.5))
    // 评分数值聚合求和
    val sumRatingLog: DataFrame = ratingLog.groupBy("user_id", "item_id")
      .agg(sum("rating"))
      .alias("sumRating")

    // 划分数据集
    val Array(trainingData, testData) = sumRatingLog.randomSplit(Array(0.8, 0.2))

    // 创建als模型
    val als: ALS = new ALS()
      .setRank(10) // 隐向量维度
      .setMaxIter(10) // 迭代次数
      .setRegParam(0.1) // 正则化
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("sumRating")

    // 训练模型
    val model: ALSModel = als.fit(trainingData)

    // 预测
    val predictions: DataFrame = model.transform(testData)

    // 评估模型性能
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("sumRating")
      .setPredictionCol("prediction")
    val rmse: Double = evaluator.evaluate(predictions)
  }
}


