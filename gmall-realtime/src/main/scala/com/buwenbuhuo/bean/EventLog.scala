package com.buwenbuhuo.bean

/**
 * Author 不温卜火
 * Create 2022-03-19 17:38
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 事件日志样例类
 */
case class EventLog(
                     mid: String,
                     uid: String,
                     appid: String,
                     area: String,
                     os: String,
                     `type`: String,
                     evid: String,
                     pgid: String,
                     npgid: String,
                     itemid: String,
                     var logDate: String,
                     var logHour: String,
                     var ts: Long
                   )

