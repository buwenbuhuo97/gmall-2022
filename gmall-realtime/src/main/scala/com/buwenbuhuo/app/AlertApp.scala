package com.buwenbuhuo.app

import com.alibaba.fastjson.JSON
import com.buwenbuhuo.bean.{CouponAlertInfo, EventLog}
import com.buwenbuhuo.constants.GmallConstants
import com.buwenbuhuo.utils.{MyEsUtil, MyKafkaUtil}
import com.ibm.icu.text.SimpleDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.util
import java.util.Date
import scala.util.control.Breaks._

/**
 * Author 不温卜火
 * Create 2022-03-19 17:53
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 预警业务主方法
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    // TODO 1.创建连接
    // 1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    // 2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    // TODO 2.核心代码
    // 1.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(
      GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    // 2.将数据转化成样例类(EventLog文档中有)，补充时间字段，将数据转换为（k，v） k->mid  v->log
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.
      map(record => {
          // 将数据转化为样例类
          val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
          // 补充日期，小时字段
          eventLog.logDate = sdf.format(new Date(eventLog.ts)).split(" ")(0)
          eventLog.logHour = sdf.format(new Date(eventLog.ts)).split(" ")(1)

          (eventLog.mid, eventLog)
    })

    /**
     * 3.开窗，开启一个5min的滑动窗口
     * 如果窗口只传一个参数，则默认滑动步长为当前批次时间
     */
    val windowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    /**
     * 4.分组聚合按照mid：将同一个窗口内相同mid的数据聚合到一块
     * 迭代器中存放的数据就是5分钟内相同的mid的数据
     */
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.groupByKey()

    /**
     * 5.筛选数据，首先用户得领优惠券，并且用户没有浏览商品行为（将符合这些行为的uid保存下来至set集合）
     * 并生成疑似预警日志
     */
    val boolDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(iter => {
      iter.map { case (mid, iter) =>
        // 创建set集合用来保存uid
        val uids: util.HashSet[String] = new util.HashSet[String]()
        // 创建set集合用来保存优惠券所涉及商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        // 创建List集合用来保存用户行为事件
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        // 创建一个标志位，用来判断用户是否有浏览商品行为
        var bool = true

        // 判断有没有浏览商品行为
        breakable {
          //遍历迭代器，获取每一个数据
          iter.foreach(log => {
            // 添加用户涉及行为
            events.add(log.evid)
            // 判断用户是否有浏览商品行为
            if (log.evid.equals("clickItem")) {
              // 一旦有浏览商品行为，则证明此5分钟内的数据不符合预警要求，则跳出循环
              bool = false
              break()
              // 判断用户是否有领取购物券行为
            } else if (log.evid.equals("coupon")) {
              /**
               * 用户没有浏览商品，但是领优惠券
               * 需要把符合要求的用户放入set集合中去重，后续通过集合的长度判断是否符合预警需求
               */
              uids.add(log.uid)
              // 添加涉及商品的id
              itemIds.add(log.itemid)
            }
          })
        }
        // 返回一个疑似预警日志(k,v)类型的数据。k（放的是用来判断是否是预警日志的条件），v（可能是预警日志的数据）
        ((uids.size() >= 3&& bool), CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    /**
     * 6.生成预警日志(将数据保存至CouponAlertInfo样例类中)，
     * 条件：符合第5步要求，并且uid个数>=3（主要为“过滤”出这些数据）
     * 实质：补全CouponAlertInfo样例类
     */
    val alertDStream: DStream[CouponAlertInfo] = boolDStream.filter(_._1).map(_._2)
    alertDStream.print(100)

    // 7.将预警数据写入ES
    alertDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{

        val indexName=GmallConstants.ES_ALERT_INDEXNAME + "-" + sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(alert => {
          // 将数据封装成kv类型。k:指的是doc_id v：指的是存放到es的数据
          (alert.mid + alert.ts / 1000 / 60, alert)
        })
        MyEsUtil.insertBulk(indexName,list)
      })
    })


    // TODO 3.开启任务并阻塞
    // 1. 开启任务
    ssc.start()
    // 2. 阻塞任务
    ssc.awaitTermination()

  }
}

