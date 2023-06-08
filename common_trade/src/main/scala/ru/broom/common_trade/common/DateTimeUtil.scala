package ru.broom.common_trade.common

import java.util.{Calendar, Date}

import ru.broom.common_trade.constants.Constants

import scala.collection.mutable.ListBuffer

object DateTimeUtil {
  def createDates(tupleInterval: (Int,Int,Int)): List[Date] = {
    val dates = new ListBuffer[Date]
    for (day <- 0 until Constants.Candle.DAYS_BEFORE_CREATE){
      val calendar = Calendar.getInstance()
      calendar.set(Calendar.HOUR_OF_DAY, 0)
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
      calendar.add(Calendar.DAY_OF_YEAR, -day)
      for (x <- 1 to tupleInterval._1) {
        dates.append(calendar.getTime)
        calendar.add(tupleInterval._2, tupleInterval._3)
      }
    }
    dates.toList
  }

  def createAroundDates(date: Date, tupleInterval: (Int,Int,Int)): (Date, Date) = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)

    for (x <- 1 to tupleInterval._1) {
      val dateBefore = calendar.getTime
      calendar.add(tupleInterval._2, tupleInterval._3)
      val dateAfter = calendar.getTime

      if (date.getTime>dateBefore.getTime && date.getTime<dateAfter.getTime){
        return (dateBefore, dateAfter)
      }
    }
    null
  }

  def createNextDate(beforeDate: Date, tupleInterval: (Int,Int,Int)): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(beforeDate)
    calendar.add(tupleInterval._2, tupleInterval._3)
    calendar.getTime
  }
}
