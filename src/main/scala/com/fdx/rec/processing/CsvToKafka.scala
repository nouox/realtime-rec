package com.fdx.rec.processing

import com.fdx.rec.utils.MyConstants
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object CsvToKafka {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("CsvToKafka")
      .getOrCreate()

    val csvPath = "path/to/csv/file"
    val brokers = "localhost:9092"
    val topic = "csv-topic"

    // 读取csv数据进dataframe
    val df: DataFrame = readCsvFiles(csvPath, MyConstants.logSchema)(spark)

    val props = new Properties()
    props.put("bootstrap.servers", brokers)

    df.toJSON.foreachPartition {
      partition: Iterator[String] =>
        val producer = new KafkaProducer[String, String](props)
        partition.foreach { record: String =>
          producer.send(new ProducerRecord[String, String](topic, record))
        }
        producer.close()
    }

    spark.stop()
  }

  def readCsvFiles(filePath: String, csvSchema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("csv")
      // .option("head", "true")
      .schema(csvSchema)
      .load(filePath)
  }
}