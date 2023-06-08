package ru.broom.binance_kafka_producer.service.`trait`

import com.fasterxml.jackson.databind.json.JsonMapper

import java.util.{Collections, Optional, Properties}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors.TopicExistsException
import ru.broom.binance_kafka_producer.config.ModuleProperties.Kafka._

import scala.util.Try

trait CommonKafkaProducer {
  protected val MAPPER: JsonMapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
  protected var properties = new Properties
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_CONFIG)
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG)
  protected val producer = new KafkaProducer[String, JsonNode](properties)
  protected val adminClient = AdminClient.create(properties)

  def dropTopic(topic: String): Unit = {
    adminClient.deleteTopics(Collections.singletonList(topic))
  }

  def createTopic(topic: String): Unit = {
    val newTopic = new NewTopic(topic, Optional.empty[Integer](), Optional.empty[java.lang.Short]());
    Try (adminClient.createTopics(Collections.singletonList(newTopic)).all.get).recover {
      case e :Exception =>
        // Ignore if TopicExistsException, which may be valid if topic exists
        if (!e.getCause.isInstanceOf[TopicExistsException]) throw new RuntimeException(e)
    }
    //println("Kafka topic - "+topic+" successful created")
  }

  protected val callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      Option(exception) match {
        case Some(err) => println(s"Failed to produce: $err")
        case None =>  //println(s"Produced record at $metadata")
      }
    }
  }

  def sendEntity(topic: String, entity: Any): Unit = {
    val json: JsonNode = MAPPER.valueToTree(entity)
    val record = new ProducerRecord[String, JsonNode](topic, json)
    producer.send(record, callback)
    producer.flush()
  }

  def close = producer.close
  def closeAdmin =  adminClient.close
}
