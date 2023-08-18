package com.fdx.rec.processing.history

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object ContentBased {
  // 1. 物品表示：将每个物品转换为特征向量，用特征向量表示物品的属性和特征。
  // 2. 用户表示：将用户对物品的评分或者点击行为转换为用户向量，用用户向量表示用户的兴趣和偏好。
  // 3. 特征相似度计算：根据特征向量计算物品之间的相似度，例如通过余弦相似度或欧几里得距离等方式计算相似度。
  // 4. 推荐生成：根据用户历史行为和物品相似度计算，为用户推荐与其历史行为相关的相似物品。
  private var spark :SparkSession = _
  def setSparkSession(spark :SparkSession): Unit = {
    this.spark = spark
  }

  def process(logDF: DataFrame, prdDF: DataFrame): ALSModel = {
    // 分词和停用词过滤
    // val tokenizer: Tokenizer = new Tokenizer()
    //   .setInputCol("title")
    //   .setOutputCol("category")
    // val stopWordsRemover: StopWordsRemover = new StopWordsRemover().setInputCol("category").setOutputCol("filtered")
    // val featurizedData: DataFrame = stopWordsRemover.transform(tokenizer.transform(product))

    // 计算TF-IDF
    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("title_arr")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)
    val arrTitleDF: DataFrame = prdDF.withColumn("title_arr", split(col("title"), " "))
    val featurizedDataWithTF: DataFrame = hashingTF.transform(arrTitleDF)

    val idf: IDF = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel: IDFModel = idf.fit(featurizedDataWithTF)
    val rescaledData: DataFrame = idfModel.transform(featurizedDataWithTF)

    // 将log中的 action 字段文本值映射为数值
    val ratingLog: DataFrame = logDF.select("userId", "itemId", "action")
      .withColumn("rating", when(
        col("action") === "pay", 1.0).otherwise(0.5))

    // 将logDF和productDF联结，得到包含特征向量和评分的DataFrame
    val joinedDF: DataFrame = ratingLog.join(rescaledData, Seq("item_id"), "left")

    // 物品相似度计算
    val documents = rescaledData.select("item_id", "features").rdd
      .map { case row => (row.getAs[Int]("item_id"), row.getAs[Vector](4,1,1)) }
    val similarities = documents.cartesian(documents).map { case ((id1, v1), (id2, v2)) => (id1, id2, cosineSimilarity(v1, v2)) }.filter { case (id1, id2, sim) => id1 != id2 }
    val similarityDF = similarities.toDF("item_id1", "item_id2", "similarity")

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

    // 返回模型
    model
  }
}
