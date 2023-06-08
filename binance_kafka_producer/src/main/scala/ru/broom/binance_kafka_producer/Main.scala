package ru.broom.binance_kafka_producer

import ru.broom.binance_kafka_producer.service.BinanceKafkaStream

object Main extends App {
  val binanceKafkaStream = new BinanceKafkaStream
  binanceKafkaStream.startStream()
}
