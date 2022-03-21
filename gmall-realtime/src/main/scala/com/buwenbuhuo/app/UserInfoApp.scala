package com.buwenbuhuo.app

import com.alibaba.fastjson.JSON
import com.buwenbuhuo.bean.UserInfo
import com.buwenbuhuo.constants.GmallConstants
import com.buwenbuhuo.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

/**
 * Author 不温卜火
 * Create 2022-03-20 13:50
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 将userinfo数据缓存至Redis
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    // TODO 1. 创建连接
    // 1. 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")
    // 2. 创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // TODO 2. 核心程序
    // 1.分别获取orderInfo的数据以及orderDetail的数据
    val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    // 2.将用户表的数据转为样例类
    val userInfoDStream: DStream[UserInfo] = KafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })

    // 3.将userInfo数据存入redis
    KafkaDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        // 创建redis连接
        val jedis: Jedis = new Jedis("hadoop01", 6379)
        partition.foreach(record => {
          // 将数据转为样例类，目的是为了提取Userid
          val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
          // 将数据转为JSON并写入Redis
          val userInfoRedisKey: String = "userInfo:"+userInfo.id
          jedis.set(userInfoRedisKey,record.value())
        })

        // 关闭连接
        jedis.close()
      })
    })

    // 打印消费数据
    userInfoDStream.print()

    // TODO 3. 线程开始等待
    ssc.start()
    ssc.awaitTermination()

  }
}
