package com.fdx.rec.processing.realtime

import com.fdx.rec.utils.{MyConstants, MyKafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object UpdateRealtimeRecModel {
  def main(args: Array[String]): Unit = {

    // 创建spark实时环境
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("update_realtime_rec_model")
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 从hdfs读取推荐模型
    val cfModel: ALSModel = ALSModel.load(MyConstants.cfModelPath)
    val cbModel: ALSModel = ALSModel.load(MyConstants.cbModelPath)

    // 从kafka中读取log数据
    val topicName: String = "rec_log";
    val groupId: String = "rec_group";
    val logKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)

    // 将Kafka数据聚合
    val sumRatingDStream: DStream[(String, String, Double)] = logKafkaDStream.map(
      record => {
        val userId: String = record.key()
        val arr: Array[String] = record.value().split("\u0001")
        val itemId: String = arr(1)
        val rating = arr(2) match {
          case "click" => 0.5
          case "collect" => 0.5
          case "cart" => 0.5
          case "buy" => 1
        }
        ((userId, itemId), rating)
      })
      .reduceByKey(_ + _)
      .map(
        record => {
          val userId: String = record._1._1
          val itemId: String = record._1._2
          val sumRating: Double = record._2
          (userId, itemId, sumRating)
        })

    sumRatingDStream.foreachRDD(
      rdd => {
        import spark.implicits._
        val df: DataFrame = rdd.toDF("userId", "itemId", "sumRating")

        // 对新数据进行预测
        val predictions: DataFrame = cfModel.transform(df)

        // 打印预测结果
        predictions.show()

        // 将新数据合并到原始数据集中，用于增量训练
        val newData: DataFrame = predictions.selectExpr("user", "item", "prediction as rating")
        val updatedData: DataFrame = cfModel.transform(newData)
        val newDataAndHistory: Dataset[Row] = updatedData.union(df)

        // 对合并后的数据进行增量训练
        val updatedModel: ALSModel = new ALS()
          .setRank(10)
          .setMaxIter(10)
          .setRegParam(0.01)
          .setUserCol("userId")
          .setItemCol("itemId")
          .setRatingCol("sumRating")
          .fit(newDataAndHistory)

        // 保存更新后的模型
        updatedModel.write.overwrite().save(MyConstants.cfModelPath)
      })

    sumRatingDStream.foreachRDD((rdd: RDD[(String, String, Double)]) => {
      val spark: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      val df: DataFrame = spark.createDataFrame(rdd)
    })
    

    // 将数据聚合处理为(userId, itemId, sumRating)格式
    val value: DStream[(String, String, Double)] = sumRatingDStream
    // 将新的数据加入到ALS模型中，并重新训练模型


    // 保存模型


    // 启动StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
