package com.buwenbuhuo.handler

import com.buwenbuhuo.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
import java.text.SimpleDateFormat
import java.util
import java.util.Date

/**
 * Author 不温卜火
 * Create 2022-03-16 1:05
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 将数据写入Redis
 */
object DauHandler {
  /**
   * 批次间去重
   * @param startUpLogDStream
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
    // 方案3（优化）：在每个批次内获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val result: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      // 1.获取redis连接
      val jedis: Jedis = new Jedis("hadoop01", 6379)
      // 2.查redis中的mid
      val rediskey = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      // 3.获取redis中的数据
      val mids: util.Set[String] = jedis.smembers(rediskey)
      // 4.将数据广播至executer端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
      // 5.根据获取到的mid去重
      val midRDD: RDD[StartUpLog] = rdd.filter(startUpLog => {
        !midBC.value.contains(startUpLog.mid)
      })
      // 6. 关闭连接
      jedis.close()
      midRDD
    })
    result
  }

  // 保存数据到Redis
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        // 创建Redis连接
        val jedis: Jedis = new Jedis("hadoop01", 6379)
        // 遍历分区内的每一条数据
        partition.foreach(startupLog=>{
          val redisKey: String = "DAU:" + startupLog.logDate
          jedis.sadd(redisKey,startupLog.mid)
        })
        // 关闭连接
        jedis.close()
      })
    })
  }
}