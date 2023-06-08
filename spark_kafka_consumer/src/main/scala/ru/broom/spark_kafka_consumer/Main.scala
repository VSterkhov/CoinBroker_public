package ru.broom.spark_kafka_consumer

import org.apache.spark.sql.functions.{avg, current_date, current_timestamp, first, last, max, min, stddev, sum, variance}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import ru.broom.common_trade.config.CommonTradeProperties.Currencies.PLAYING_CURRENCIES_ARRAY
import ru.broom.common_trade.model.{CallOrder, PerfectOrder, Price}
import ru.broom.spark_kafka_consumer.service.{HiveService, KafkaStreamingService}
import ru.broom.spark_kafka_consumer.utils.SparkBeanUtils

// Debug conf - spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005

object Main extends App {

  val priceTopics: Array[String] = PLAYING_CURRENCIES_ARRAY.map(cur => "PRICE_" + cur)
  val askCallTopics: Array[String] = PLAYING_CURRENCIES_ARRAY.map(cur => "ASK_" + cur)
  val bidCallTopics: Array[String] = PLAYING_CURRENCIES_ARRAY.map(cur => "BID_" + cur)
  val sellPerfectTopics: Array[String] = PLAYING_CURRENCIES_ARRAY.map(cur => "SELL_" + cur)
  val buyPerfectTopics: Array[String] = PLAYING_CURRENCIES_ARRAY.map(cur => "BUY_" + cur)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Kafka Consumer")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    // Todo -- https://stackoverflow.com/questions/76239919/spark-saveastable-overwrites-data-in-cluster-mode
    .config("spark.sql.sources.partitionOverwriteMode","dynamic")
    .config("dfs.block.size", "32m")
    .enableHiveSupport()
    .getOrCreate()

  val streamingContext = new StreamingContext(spark.sparkContext, Seconds(60))
  val hiveService = new HiveService(spark)
  hiveService.createDatabaseIfNotExists("coin_broker")

  val kafkaStreamingService = new KafkaStreamingService(spark, streamingContext)

  val priceSchema = SparkBeanUtils.getSchemaFromBean(classOf[Price])
  val callOrderSchema = SparkBeanUtils.getSchemaFromBean(classOf[CallOrder])
  val perfectOrderSchema = SparkBeanUtils.getSchemaFromBean(classOf[PerfectOrder])

  val priceDStream = kafkaStreamingService.createKafkaDStreamOfClassTag[Price](priceTopics, priceSchema)
  val askCallDStream = kafkaStreamingService.createKafkaDStreamOfClassTag[CallOrder](askCallTopics, callOrderSchema)
  val bidCallDStream = kafkaStreamingService.createKafkaDStreamOfClassTag[CallOrder](bidCallTopics, callOrderSchema)
  val sellPerfectDStream = kafkaStreamingService.createKafkaDStreamOfClassTag[PerfectOrder](sellPerfectTopics, perfectOrderSchema)
  val buyPerfectDStream = kafkaStreamingService.createKafkaDStreamOfClassTag[PerfectOrder](buyPerfectTopics, perfectOrderSchema)


  println("Waiting 60 seconds for streams are given records...")
  Thread.sleep(Seconds(60).milliseconds)

  val tradeVolumeDStream = kafkaStreamingService.joinDStreamRDDs(Seq("currency"), Array(
    priceDStream.transform(rdd=>{
      spark.createDataFrame(rdd, priceSchema.getSparkSchema)
        .groupBy("currency")
        .agg(
          first("cost").alias("first_cost"),
          last("cost").alias("last_cost"),
          max("cost").alias("max_cost"),
          min("cost").alias("min_cost")
        ).rdd
    }),
    bidCallDStream.transform(rdd=>{
      spark.createDataFrame(rdd, callOrderSchema.getSparkSchema)
        .distinct()
        .groupBy("currency")
        .agg(
          avg("cost").alias("bid_avg_cost"),
          stddev("cost").alias("bid_std_cost"),
          variance("cost").alias("bid_var_cost"),
          sum("quantity").alias("bid_sum_quantity"),
          avg("quantity").alias("bid_avg_quantity"),
          stddev("quantity").alias("bid_std_quantity"),
          variance("quantity").alias("bid_var_quantity")
        ).rdd
    }),
    askCallDStream.transform(rdd=>{
    spark.createDataFrame(rdd, callOrderSchema.getSparkSchema)
      .distinct()
      .groupBy("currency")
      .agg(
        avg("cost").alias("ask_avg_cost"),
        stddev("cost").alias("ask_std_cost"),
        variance("cost").alias("ask_var_cost"),
        sum("quantity").alias("ask_sum_quantity"),
        avg("quantity").alias("ask_avg_quantity"),
        stddev("quantity").alias("ask_std_quantity"),
        variance("quantity").alias("ask_var_quantity")
      ).rdd}),
    sellPerfectDStream.transform(rdd=>{
      spark.createDataFrame(rdd, perfectOrderSchema.getSparkSchema)
        .groupBy("currency")
        .agg(
          avg("cost").alias("sell_avg_cost"),
          stddev("cost").alias("sell_std_cost"),
          variance("cost").alias("sell_var_cost"),
          sum("quantity").alias("sell_sum_quantity"),
          avg("quantity").alias("sell_avg_quantity"),
          stddev("quantity").alias("sell_std_quantity"),
          variance("quantity").alias("sell_var_quantity")
        ).rdd
    }),
    buyPerfectDStream.transform(rdd=>{
      spark.createDataFrame(rdd, perfectOrderSchema.getSparkSchema)
        .groupBy("currency")
        .agg(
          avg("cost").alias("buy_avg_cost"),
          stddev("cost").alias("buy_std_cost"),
          variance("cost").alias("buy_var_cost"),
          sum("quantity").alias("buy_sum_quantity"),
          avg("quantity").alias("buy_avg_quantity"),
          stddev("quantity").alias("buy_std_quantity"),
          variance("quantity").alias("buy_var_quantity")
        ).rdd
    })
  )).transform(rdd => {
    val schema = rdd.first().schema
    val dataset = spark.createDataFrame(rdd, schema)
      .withColumn("current_time", current_timestamp())
      .withColumn("current_day", current_date())
    dataset.rdd
  })

  var firstWrite = true
  tradeVolumeDStream.foreachRDD(rdd=>{
    val firstRow = rdd.first()
    val schema = firstRow.schema

    val dataset = spark.createDataFrame(rdd, schema)
    dataset.write
      .mode("append")
      .format("orc")
      .option("path", "/coin_broker/trade_volume")
      .partitionBy("current_day", "currency")
      .saveAsTable("coin_broker.trade_volume")

    if (firstWrite){
      hiveService.setManagedTable("trade_volume", "coin_broker")
      firstWrite = false
    }
  })

  streamingContext.start()
  streamingContext.awaitTermination()


}

