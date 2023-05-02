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
    // 从hdfs读数据
    val log: DataFrame = spark.read
      .format("txt")
      .option("header", "false")
      .schema(MyConstants.logSchema)
      .load(MyConstants.logPath)

    // 数据预处理
    val logProcessed: DataFrame = log.select(
      col("user_id"),
      col("item_id"),
      expr("action")
    )

    // 划分数据集
    val Array(trainingData, testData) = log.randomSplit(Array(0.8, 0.2))

    // 创建als模型
    val als: ALS = new ALS()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("action")

    // 训练模型
    val model: ALSModel = als.fit(trainingData)

    // 预测
    val predictions: DataFrame = model.transform(testData)

    // 评估模型性能
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("action")
      .setPredictionCol("predictioin")
    val rmse: Double = evaluator.evaluate(predictions)
  }
}


