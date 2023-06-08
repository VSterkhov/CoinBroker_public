package ru.broom.common_trade.constants

import java.util.Calendar

object Constants {
  object Candle {
    val DAYS_BEFORE_CREATE = 1
    object Intervals {
      // counter, stepType, step
      val ONE_MINUTE_INTERVAL = (1440, Calendar.MINUTE, 1)
      val FIFTY_MINUTES_INTERVAL = (96, Calendar.MINUTE, 15)
      val HOUR_INTERVAL = (24, Calendar.HOUR, 1)
      val FOUR_HOUR_INTERVAL = (6, Calendar.HOUR, 4)
      val DAILY_INTERVAL = (1, Calendar.DAY_OF_YEAR, 1)
    }
  }
}
