package com.buwenbuhuo.app

import com.alibaba.fastjson.JSON
import com.buwenbuhuo.bean.StartUpLog
import com.buwenbuhuo.constants.GmallConstants
import com.buwenbuhuo.handler.DauHandler
import com.buwenbuhuo.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Author 不温卜火
 * Create 2022-03-15 20:56
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 批次间去重（方法3）
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    // TODO 1. 创建连接
    // 1. 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    // 2. 创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // TODO 2. 核心程序
    // 1. 消费Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    // 2. 将获取到的json格式数据转为样例类，并补全logDate & logHour字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        // 将json数据转换为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        // 补充字段分别为 logdate loghour 样式为：yyyy-MM-dd HH
        val str: String = sdf.format(new Date(startUpLog.ts))
        // 补充字段 logdate yyyy-MM-dd
        startUpLog.logDate = str.split(" ")(0)
        // 补充字段 loghour HH
        startUpLog.logHour = str.split(" ")(1)
        // 返回样例类
        startUpLog
      })
    })

    // 对startUpLogDStream做缓存（小优化）
    startUpLogDStream.cache()
    // 原始数据条数
    startUpLogDStream.count().print()

    // 3. 做批次间去重
    val fileterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)
    // 对fileterByRedisDStream做缓存（小优化）
    fileterByRedisDStream.cache()

    // 经过批次间去重后的数据条数
    fileterByRedisDStream.count().print()

    // 4. 做批次内去重


    // 5. 将去重的结果写入redis中
    DauHandler.saveToRedis(fileterByRedisDStream)

    // 6. 将去重后的明细数据写入hbase

    // TODO 3.结束
    // 1. 开启任务
    ssc.start()
    // 2. 阻塞任务
    ssc.awaitTermination()
  }
}