package com.buwenbuhuo.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * Author 不温卜火
 * Create 2022-03-14 23:47
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 读取配置文件
 */
object PropertiesUtil {
  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().
      getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

  /*
  // 这是一个测试类：读取properties配置文件的内容
  def main(args:Array[String]):Unit={
    val properties: Properties = load("config.properties")
    val str: String = properties.getProperty("kafka.broker.list")
    println(str)
  }
  */
}
