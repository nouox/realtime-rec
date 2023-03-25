package com.fdx.rec.processing

import com.fdx.rec.utils.MyConstants
import org.apache.spark.sql.SparkSession

object GenRecModel {
  def main(args: Array[String]): Unit = {

    // create sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("gen_offline_rec_model")
      .getOrCreate()

    // read csv from hdfs
    val data = spark.read.format("csv")
      .option("header", "true")
      .schema(MyConstants.logSchema)
      .load(MyConstants.logCsvPath)

    // create recommendation model


    // close spaekSession
    spark.stop()
  }
}