package tech.elephant.spark

import grizzled.slf4j.Logger
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object SimplestStreaming {
  val logger = Logger[this.type]

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Simplest streaming (spark 2.0) from Kafka SSL")
      .enableHiveSupport()
      .getOrCreate()
    val sparkContext = spark.sparkContext

    val streamingContext = new StreamingContext(sparkContext, Seconds(10))
    // expects jaas.conf, appropriate keytab, and kafka.client.truststore.jks passed in as part of spark-submit
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "<fqdn of kafka broker>:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> "SASL_SSL",
      "ssl.truststore.location" -> "./kafka.client.truststore.jks",
      "ssl.truststore.password" -> "change-me-to-something-safe"
    )
    val topic = Set("simplest")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val df = rdd.map( consumerRecord => {
        consumerRecord.value()
      }).toDF()

      df.show()
    }

    // start the computation
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
