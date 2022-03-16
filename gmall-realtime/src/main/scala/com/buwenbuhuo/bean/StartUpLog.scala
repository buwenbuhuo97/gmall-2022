package com.buwenbuhuo.bean

/**
 * Author 不温卜火
 * Create 2022-03-15 21:23
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */
case class StartUpLog(
                       mid: String,
                       uid: String,
                       appid: String,
                       area: String,
                       os: String,
                       ch: String,
                       `type`: String,
                       vs: String,
                       /**
                        * logDate:日期 & logHour:小时
                        * 这两个字段是数据中所没有的，添加这两个字段的作用是
                        *   1.计算日活
                        *   2.方便可视化
                        */
                       var logDate: String,
                       var logHour: String,
                       var ts: Long
                     )

