package ru.broom.common_trade.config

import java.util.Properties

object CommonTradeProperties {
  private val properties = new Properties()
  properties.load(getClass.getResource("/common-trade.properties").openStream())

  object Candle {
    val RANGE_ITEMIZATION_MILLIS: Int = properties.getProperty("candle.itemization.range").toInt
  }
  object Currencies {
    val PLAYING_CURRENCIES_ARRAY: Array[String] = properties.getProperty("currencies").split(",")
  }

}
