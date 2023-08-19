package com.fdx.rec.processing.history

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ContentBased {
  private var spark: SparkSession = _

  def setSparkSession(spark: SparkSession): Unit = {
    this.spark = spark
  }

  def process(logDF: DataFrame, prdDF: DataFrame): DataFrame = {
    // TODO 暂时使用相同词个数来计算相似性，后续引入相关算法计算词语相似性
    // 定义udf：求两商品title中相同词的个数
    val similarityUDF = udf { (title1: String, title2: String) =>
      val words1 = title1.toLowerCase().split(" ")
      val words2 = title2.toLowerCase().split(" ")
      words1.toSet.intersect(words2.toSet).size
    }

    // 自关联求笛卡尔积，计算任意两商品title中相同词的个数
    val similarityData = prdDF.alias("a")
      .crossJoin(prdDF.alias("b"))
      .select(
        col("a.prdId").alias("aid"),
        col("a.title").alias("aTitle"),
        col("b.prdId").alias("bid"),
        col("b.title").alias("bTitle"),
        (similarityUDF(col("a.title"), col("b.title")) + rand()).alias("CommonWordCnt")
      )
      .filter(col("aid") =!= col("bid"))

    // 定义开窗函数：按aid分组，按CommonWordCnt（相同词的个数）降序排列
    val windowSpec: WindowSpec = Window.partitionBy("aid").orderBy(col("CommonWordCnt").desc)

    // 按aid分组，取每组前5个bid，即aid商品的top5相似的bid商品
    val top5SimilarPrds: Dataset[Row] = similarityData
      .withColumn("Rank", rank().over(windowSpec))
      .filter(col("Rank") <= 5)
      .orderBy(col("aid"))

    // 只需保存aid与bid的映射关系
    top5SimilarPrds.select(col("aid"), col("bid"))
  }


}
