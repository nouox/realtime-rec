package com.fdx.rec.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

object MyKafkaUtils {
  /**
   * 消费者配置
   *
   * ConsumerConfig
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    // kafka集群位置
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    // kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> MyPropsUtils(MyConfig.KEY_DESERIALIZER_CLASS),
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> MyPropsUtils(MyConfig.VALUE_DESERIALIZER_CLASS),
    // groupId
    // offset提交 自动 手动
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // 自动提交的时间间隔
    // ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    // offset重置 "latest" "earliest"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  );

  /**
   * 基于SparkStreaming消费 ,获取到KafkaDStream , 使用默认的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }

  /**
   * 基于SparkStream消费，获取到kafkaDStream，使用指定的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets)
    )
    kafkaDStream
  }

  /**
   * 生产者对象
   */
  val producer: KafkaProducer[String, String] = createProducer()

  /**
   * 创建生产者对象
   */
  def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs = new util.HashMap[String, AnyRef]
    // 生产者配置类ProducerConfig
    // kafka集群位置
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    // kv序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MyPropsUtils(MyConfig.KEY_SERIALIZER_CLASS))
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyPropsUtils(MyConfig.VALUE_SERIALIZER_CLASS))
    // acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, MyPropsUtils(MyConfig.ACKS))
    // batch.size
    // linger.ms
    // retries
    // 幂等配置
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, MyPropsUtils(MyConfig.ENABLE_IDEMPOTENCE))

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  // TODO 查看kafka分区策略

  /**
   * 生产（按照默认的黏性分区策略）
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * 生产（按照key进行分区）
   */
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * 关闭
   */
  def close(): Unit = {
    if (producer != null) producer.close()
  }

  /**
   * 刷写
   */
  def flush(): Unit = {
    producer.flush()
  }
}
