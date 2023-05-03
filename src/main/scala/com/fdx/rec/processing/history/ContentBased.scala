package com.fdx.rec.processing.history

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ContentBased {
  def process(spark: SparkSession, log: DataFrame, product: DataFrame): Unit = {
    // 数据预处理
    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("title")
      .setOutputCol("words")
    val wordsData: DataFrame = tokenizer.transform(product)

    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)
    val featurizedData: DataFrame = hashingTF.transform(wordsData)

    val idf: IDF = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val rescaledData: DataFrame = idf.fit(featurizedData).transform(featurizedData)

    // 将log中的action映射为数值
    val ratingLog: DataFrame = log.select("userId", "itemId", "action")
      .withColumn("rating", when(
        col("action") === "pay", 1.0).otherwise(0.5))

    // 将logDF和productDF联结，得到包含特征向量和评分的DataFrame
    val joinedDF: DataFrame = ratingLog.join(rescaledData, Seq("item_id"), "left")

    // 构建als模型
    val als: ALS = new ALS()
      .setMaxIter(10)
      .setRank(10)
      .setRegParam(0.1)
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("rating")
    val model: ALSModel = als.fit(joinedDF)

    // 使用训练好的模型进行预测，并对预测结果进行评估
    val predictions: DataFrame = model.transform(joinedDF)
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse: Double = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
  }
}
