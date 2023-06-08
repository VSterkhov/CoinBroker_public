package ru.broom.common_trade.model

import java.sql.Timestamp

case class Price() extends Serializable {

  override def toString: String = {
    "{currency: " + currency + "\n" +
      "cost: " + cost + "\n" +
      "current_time: " + current_time + "}"
  }

  def this(currency: String, cost: Double, current_time: Timestamp) {
    this()
    this.currency = currency
    this.cost = cost
    this.current_time = current_time
  }

  var currency: String = _
  var cost: Double = _
  var current_time: Timestamp = _

  def getCurrency: String = currency
  def getCost: Double = cost
  def getCurrent_time: Timestamp = current_time
  def setCurrency(currency: String): Unit = this.currency = currency
  def setCost(cost: Double): Unit = this.cost = cost
  def setCurrent_time(current_time: Timestamp): Unit = this.current_time = current_time

}
