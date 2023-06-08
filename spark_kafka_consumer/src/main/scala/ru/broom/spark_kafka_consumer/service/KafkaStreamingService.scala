package ru.broom.spark_kafka_consumer.service

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import ru.broom.spark_kafka_consumer.utils.{Schema, SparkBeanUtils}

import scala.reflect.ClassTag

class KafkaStreamingService(spark: SparkSession, streamingContext: StreamingContext) {

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:29092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "coin_broker",
    //"client.id" -> "spark_streaming_consumer",
    "auto.offset.reset" -> "latest",//"earliest",
    //"auto.commit.interval.ms" -> "10000",
    //"max.poll.interval.ms" -> "60000",
    //"max.poll.records" -> "",
    //"session.timeout.ms" -> "60000",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  private def createKafkaDStream(topics: Array[String]): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
  }

  def createKafkaDStreamOfClassTag[T:ClassTag](topics: Array[String], schema: Schema): DStream[Row] = {
    val ct = scala.reflect.classTag[T].runtimeClass
    createKafkaDStream(topics).map(message=> {
      val jsonMapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
      val bean = jsonMapper.readValue(message.value(), ct).asInstanceOf[T]
      SparkBeanUtils.getRowFromBean(schema, bean)
    })
  }

  private def joinRDDs(colNames: Seq[String]): (RDD[Row], RDD[Row]) => RDD[Row] = { (leftRDD, rightRDD) =>
    val leftSchema = leftRDD.first().schema
    val rightSchema = rightRDD.first().schema
    val leftDataset = spark.createDataFrame(leftRDD,leftSchema)
    val rightDataset = spark.createDataFrame(rightRDD, rightSchema)
    leftDataset.join(rightDataset, colNames).rdd
  }

  def joinDStreamRDDs(colName: Seq[String], streams: Array[DStream[Row]]): DStream[Row] = {
    streams.reduce((s1,s2)=>{
      s1.transformWith(s2, joinRDDs(colName))
    })
  }
}
