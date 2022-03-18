package com.buwenbuhuo.app

import com.alibaba.fastjson.JSON
import com.buwenbuhuo.bean.OrderInfo
import com.buwenbuhuo.constants.GmallConstants
import com.buwenbuhuo.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author 不温卜火
 * Create 2022-03-18 11:40
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    // TODO 1.创建连接
    // 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GmvApp")
    // 2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // TODO 2.核心代码
    // 1.获取Kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    // 2.把JSON格式的数据转为样例类，并补全字段
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        // 将数据转为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        // 补全时间字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        // 对手机号做脱敏操作
        orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "*******"

        // 返回样例类
        orderInfo
      })
    })

    // 3.将数据写入Hbase
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL2022_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop01,hadoop02,hadoop03:2181")
      )
    })

    // TODO 3. 线程开始等待
    ssc.start()
    ssc.awaitTermination()
  }
}
