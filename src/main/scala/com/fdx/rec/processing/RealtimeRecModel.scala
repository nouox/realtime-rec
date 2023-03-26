package com.fdx.rec.processing

import com.fdx.rec.utils.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeRecModel {
  def main(args: Array[String]): Unit = {

    // 1 create realtime environment
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("realtime_rec_model")
      .setMaster("local[4]")
      .set("spark.driver.host", "localhost")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2 read data from kafka
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc, "rec_log", "")

    // 3 process data


    // 4 train recommendation model


    // 5 save model


    // start StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
