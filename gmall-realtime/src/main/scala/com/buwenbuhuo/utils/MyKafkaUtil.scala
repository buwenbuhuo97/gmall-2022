package com.buwenbuhuo.utils

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


/**
 * Author 不温卜火
 * Create 2022-03-14 23:54
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 消费Kafka的数据
 */
object MyKafkaUtil {
  // TODO 1.创建配置信息对象
  private val properties: Properties = PropertiesUtil.load("config.properties")

  // TODO 2.用于初始化链接到集群的地址
  val broker_list: String = properties.getProperty("kafka.broker.list")

  // TODO 3.kafka消费者配置
  /**
   * 为什么选Map：源码提示
   *  Ctrl+鼠标左键点击kafkaParam，自动进入Subscribe方法引入，再点进Subscribe方法发现其为一个Map
   *  部分源码如下：kafkaParams: collection.Map[String, Object]): ConsumerStrategy[K, V]
   */
  val kafkaParam = Map(
    "bootstrap.servers" -> broker_list,
    // 如何使用正反序列化：生产者序列化，消费者反序列化。其为消费者组特有
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    // 消费者组，此处正常不应该写死
    "group.id" -> "bigdata20220320",
    /**
      如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      可以使用这个配置，latest自动重置偏移量为最新的偏移量
     */
    "auto.offset.reset" -> "latest",
    /**
      是否自动维护偏移量
        如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
        如果是false，会需要手动维护kafka偏移量
     */
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  // TODO 4. 创建DStream，返回接收到的输入数据
  /**
   *  LocationStrategies：根据给定的主题和集群地址创建consumer
   *  LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
   *  ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
   *  ConsumerStrategies.Subscribe：订阅一系列主题
   */
  def getKafkaStream(topic: String, ssc: StreamingContext):
  InputDStream[ConsumerRecord[String, String]] = {
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    dStream
  }
}

