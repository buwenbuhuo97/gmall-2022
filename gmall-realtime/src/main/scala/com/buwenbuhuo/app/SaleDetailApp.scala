package com.buwenbuhuo.app

import com.alibaba.fastjson.JSON
import com.buwenbuhuo.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.buwenbuhuo.constants.GmallConstants
import com.buwenbuhuo.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization
import java.util

/**
 * Author 不温卜火
 * Create 2022-03-20 13:32
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:双流Join完整demo
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    // TODO 1. 创建连接
    // 1. 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    // 2. 创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // TODO 2. 核心程序
    // 1.消费kafka中的数据：分别获取orderInfo(订单表)的数据以及orderDetail(订单明细表)的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    // 2. 将两条流的数据分别转为样例类
    val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        // 将数据转为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        // 补全时间字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        // 对手机号做脱敏操作
        orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "*****" +
          orderInfo.consignee_tel.substring(8, 11)

        // 返回样例类
        (orderInfo.id,orderInfo)
      })
    })

    val orderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(),
          classOf[OrderDetail])

        (orderDetail.order_id,orderDetail)
      })
    })

    // 3. 双流join
    /*val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
    value.print()*/
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream
      .fullOuterJoin(orderDetailDStream)

    // 4.利用加缓存的方式解决因网络延迟所带来的数据丢失问题
    val noUserSalDetail: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      // 定义details当作mapPartitions的返回值
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      // 创建Redis连接
      val jedis: Jedis = new Jedis("hadoop01", 6379)

      partition.foreach { case (orderId, (infoOpt, detailOpt)) =>
        // 定义好RedisKey
        val orderInfoRedisKey: String = "orderInfo:" + orderId
        val orderDetailRedisKey: String = "orderDetail:" + orderId

        // 1.判断OrderInfo数据是否存在
        if (infoOpt.isDefined) {
          // orderInfo数据存在
          val orderInfo: OrderInfo = infoOpt.get
          // 2.判断orderDetail数据是否存在
          if (detailOpt.isDefined) {
            // orderDetail数据存在
            val orderDetail: OrderDetail = detailOpt.get
            // 将关联上的数据组合成为SaleDetail
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            // 并将SaleDetail写入结果集合
            details.add(saleDetail)
          }
          // 3. 将自己(orderInfo)数据写入缓存
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(orderInfoRedisKey, orderInfoJson)
          // 设置过期时间,通常过期时间是网络延迟事件（时间推荐大一点，手动认为模拟时间和时间有差距）
          jedis.expire(orderInfoRedisKey, 20)

          /**
           * 4.去对方缓存（orderDetail）中查寻有没有能关联上的数据
           * 先判断对方缓存中是否有对应的redisKey，有的话再将数据查询出来，并关联
           */
          if (jedis.exists(orderDetailRedisKey)) {
            // 证明有能关联上orderDetail的数据
            val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailRedisKey)

            /**
             * 先导入import collection.JavaConverters._，然后可以调用xxx.asScala
             * 这个方法可以将java集合转为Scala集合
             */
            import collection.JavaConverters._
            for (elem <- orderDetailSet.asScala) {
              // 将查询出来的OrderDetail字符串转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            }
          }
        } else {
          // orderInfo不存在
          // 5.判断orderDetail是否存在
          if (detailOpt.isDefined) {
            // orderDetail存在
            val orderDetail: OrderDetail = detailOpt.get
            // 6.查询对方缓存(orderInfo)中是否有能够关联上的数据
            if (jedis.exists(orderInfoRedisKey)) {
              // 有能够关联上的数据
              val orderInfoStr: String = jedis.get(orderInfoRedisKey)
              // 将查询出来的JSON字符串转换为样例类
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            } else {
              // 如果没有能够关联上的数据，则将自己写入缓存
              val orderDetailStr: String = Serialization.write(orderDetail)
              jedis.sadd(orderDetailRedisKey, orderDetailStr)
              // 对orderDetail数据设置过期时间
              jedis.expire(orderDetailRedisKey, 100)
            }
          }
        }
      }
      // 关闭连接
      jedis.close()
      import collection.JavaConverters._
      details.iterator().asScala
    })

    // 5. 查询Redis缓存，并关联userInfo数据
    val saleDetailDStream: DStream[SaleDetail] = noUserSalDetail.mapPartitions(partition => {
      // 创建redis连接
      val jedis: Jedis = new Jedis("hadoop01", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        // 根据redisKey查询对应的用户数据
        val userInfoRedisKey: String = "userInfo:" + saleDetail.user_id
        val userInfoStr: String = jedis.get(userInfoRedisKey)
        // 将读取过来的字符串转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      // 关闭连接
      jedis.close()
      details
    })

    // 打印流
    saleDetailDStream.print()

    // 6.将数据保存到es
    saleDetailDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(partition =>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_DETAIL_INDEXNAME + "0320",list)
      })
    })


    // TODO 3. 线程开始等待
    ssc.start()
    ssc.awaitTermination()
  }
}
