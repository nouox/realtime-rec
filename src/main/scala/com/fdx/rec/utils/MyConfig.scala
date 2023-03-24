package com.fdx.rec.utils

object MyConfig {
  val KAFKA_BOOTSTRAP_SERVERS: String = "kafka.bootstrap-servers"
  val KEY_DESERIALIZER_CLASS: String = "kafka.key-deserializer-class"
  val VALUE_DESERIALIZER_CLASS: String = "kafka.value-deserializer-class"
  val KEY_SERIALIZER_CLASS: String = "kafka.key-serializer-class"
  val VALUE_SERIALIZER_CLASS: String = "kafka.value-serializer-class"
  val ACKS: String = "kafka.acks"
  val ENABLE_IDEMPOTENCE: String = "kafka.enable-idempotence"
}
