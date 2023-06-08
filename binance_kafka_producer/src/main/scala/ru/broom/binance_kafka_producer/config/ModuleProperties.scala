package ru.broom.binance_kafka_producer.config

import java.util.Properties

object ModuleProperties {

  private def notNullOrEmpty(s: String): Boolean = {
    s!=null && s.nonEmpty
  }

  private val properties = new Properties()
  properties.load(getClass.getResource("/module.properties").openStream())

  object BinanceCredentials {
    val API_KEY = properties.getProperty("binance.credentials.apikey")
    val SECRET = properties.getProperty("binance.credentials.secret")
  }

  object AskBinance {
    val PRICE_ASK_TIMEOUT_MILLISECONDS: Int = properties.getProperty("binance.ask.price.timeout").toInt
    val CALL_ORDER_ASK_TIMEOUT_MILLISECONDS: Int = properties.getProperty("binance.ask.callorder.timeout").toInt
    val CALL_ORDER_ASK_STACK_COUNT: Int = properties.getProperty("binance.ask.callorder.stack").toInt
    val PERFECT_ORDER_ASK_TIMEOUT_MILLISECONDS: Int = properties.getProperty("binance.ask.perfectorder.timeout").toInt
    val PERFECT_ORDER_ASK_STACK_COUNT: Int = properties.getProperty("binance.ask.perfectorder.stack").toInt
  }

  object Kafka {
    val BOOTSTRAP_SERVERS_CONFIG: String = if (notNullOrEmpty(System.getenv("BOOTSTRAP_SERVERS_CONFIG"))) System.getenv("BOOTSTRAP_SERVERS_CONFIG") else properties.getProperty("default.bootstrap.servers")
    val KEY_SERIALIZER_CLASS_CONFIG: String = if (notNullOrEmpty(System.getenv("KEY_SERIALIZER_CLASS_CONFIG")))  System.getenv("KEY_SERIALIZER_CLASS_CONFIG") else properties.getProperty("default.key.serializer")
    val VALUE_SERIALIZER_CLASS_CONFIG: String = if (notNullOrEmpty(System.getenv("VALUE_SERIALIZER_CLASS_CONFIG")))  System.getenv("VALUE_SERIALIZER_CLASS_CONFIG") else properties.getProperty("default.value.serializer")
    println("BOOTSTRAP_SERVERS_CONFIG:"+BOOTSTRAP_SERVERS_CONFIG)
    println("KEY_SERIALIZER_CLASS_CONFIG:"+KEY_SERIALIZER_CLASS_CONFIG)
    println("VALUE_SERIALIZER_CLASS_CONFIG:"+VALUE_SERIALIZER_CLASS_CONFIG)
  }
}
