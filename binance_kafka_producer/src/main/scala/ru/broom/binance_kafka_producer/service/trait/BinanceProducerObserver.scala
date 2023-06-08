package ru.broom.binance_kafka_producer.service.`trait`

import com.binance.api.client.domain.market.{OrderBook, TickerPrice}
import ru.broom.binance_kafka_producer.config.ModuleProperties.AskBinance._
import ru.broom.common_trade.model.{CallOrder, PerfectOrder, Price}
import ru.broom.common_trade.config.CommonTradeProperties.Currencies.PLAYING_CURRENCIES_ARRAY

import scala.collection.mutable.ListBuffer
import rx.subjects.BehaviorSubject

import java.sql.Timestamp

trait BinanceProducerObserver extends BinanceConnection {

  protected val currenciesPriceObserverMap: Map[String, BehaviorSubject[Price]] =
    PLAYING_CURRENCIES_ARRAY.map(currency=>{("PRICE_"+currency, BehaviorSubject.create[Price]())}).toMap

  protected val bidCallOrdersObserverMap: Map[String, BehaviorSubject[CallOrder]] =
    PLAYING_CURRENCIES_ARRAY.map(currency=>{("BID_"+currency, BehaviorSubject.create[CallOrder]())}).toMap

  protected val askCallOrdersObserverMap: Map[String, BehaviorSubject[CallOrder]] =
    PLAYING_CURRENCIES_ARRAY.map(currency=>{("ASK_"+currency, BehaviorSubject.create[CallOrder]())}).toMap

  protected val sellPerfectOrdersObserverMap: Map[String, BehaviorSubject[PerfectOrder]] =
    PLAYING_CURRENCIES_ARRAY.map(currency=>{("SELL_"+currency, BehaviorSubject.create[PerfectOrder]())}).toMap

  protected val buyPerfectOrdersObserverMap: Map[String, BehaviorSubject[PerfectOrder]] =
    PLAYING_CURRENCIES_ARRAY.map(currency=>{("BUY_"+currency, BehaviorSubject.create[PerfectOrder]())}).toMap


  new Thread(() => {
    Thread.sleep(10000)
    while(true) {
      try {
        val timestamp = binanceClient.getServerTime
        for (ticker <- filteringCurrencies(binanceClient.getAllPrices)) {
          val behaviorSubject = currenciesPriceObserverMap("PRICE_" + ticker.getSymbol)
          behaviorSubject.onNext(new Price(ticker.getSymbol, ticker.getPrice.toDouble, new Timestamp(timestamp)))
        }
        Thread.sleep(PRICE_ASK_TIMEOUT_MILLISECONDS)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          Thread.sleep(3000)
        }
      }
    }
  }).start()

  new Thread(() => {
    Thread.sleep(10000)
    while(true) {
      try {
        for (currency <- PLAYING_CURRENCIES_ARRAY){
          val orderBook: OrderBook = binanceClient.getOrderBook(currency, CALL_ORDER_ASK_STACK_COUNT)

          new Thread(() => {
            val bidCallBehaviorSubject = bidCallOrdersObserverMap("BID_" + currency)
            orderBook.getBids.forEach(bids => {
              bidCallBehaviorSubject.onNext(new CallOrder(currency, bids.getPrice.toDouble, bids.getQty.toFloat))
            })
          }).start()

          new Thread(() => {
            val askCallBehaviorSubject = askCallOrdersObserverMap("ASK_" + currency)
            orderBook.getAsks.forEach(ask => {
              askCallBehaviorSubject.onNext(new CallOrder(currency, ask.getPrice.toDouble, ask.getQty.toFloat))
            })
          }).start()

        }
        Thread.sleep(CALL_ORDER_ASK_TIMEOUT_MILLISECONDS)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          Thread.sleep(3000)
        }
      }
    }
  }).start()

  new Thread(() => {
    Thread.sleep(10000)
    while(true) {
      try {
        for (currency <- PLAYING_CURRENCIES_ARRAY){
          val startTime: Long = System.currentTimeMillis()-PERFECT_ORDER_ASK_TIMEOUT_MILLISECONDS
          val endTime: Long = System.currentTimeMillis()
          new Thread(() => {

            val sellPerfectOrdersBehaviorSubject = sellPerfectOrdersObserverMap("SELL_"+currency)
            val buyPerfectOrdersBehaviorSubject = buyPerfectOrdersObserverMap("BUY_"+currency)
            val aggTrades = binanceClient.getAggTrades(currency,null,PERFECT_ORDER_ASK_STACK_COUNT, startTime, endTime)

            aggTrades.forEach(trade=>{
              val order = new PerfectOrder(currency, trade.getPrice.toDouble,trade.getQuantity.toFloat)
              if (trade.isBuyerMaker)
                sellPerfectOrdersBehaviorSubject.onNext(order)
              else
                buyPerfectOrdersBehaviorSubject.onNext(order)
            })

          }).start()
        }
        Thread.sleep(PERFECT_ORDER_ASK_TIMEOUT_MILLISECONDS)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          Thread.sleep(3000)
        }
      }
    }
  }).start()

  private def filteringCurrencies(tickers: java.util.List[TickerPrice]): List[TickerPrice] = {
    val listBuffer = new ListBuffer[TickerPrice]
    tickers.forEach(ticker=>{
      if (PLAYING_CURRENCIES_ARRAY.contains(ticker.getSymbol))
        listBuffer.append(ticker)
    })
    listBuffer.toList
  }
}



