package ru.broom.common_trade.model

class PerfectOrder extends Serializable {
  def this(currency: String, cost: Double, quantity: Double) {
    this()

    this.currency=currency
    this.cost=cost
    this.quantity=quantity
  }

  var currency: String = _
  var cost: Double = _
  var quantity: Double = _

  def getCurrency: String = currency
  def getCost: Double = cost
  def getQuantity: Double = quantity

  def setCurrency(currency: String): Unit = this.currency=currency
  def setCost(cost: Double): Unit = this.cost=cost
  def setQuantity(quantity: Double): Unit = this.quantity=quantity
}
