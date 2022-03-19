package com.buwenbuhuo.bean

/**
 * Author 不温卜火
 * Create 2022-03-19 17:44
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 警告日志存放样例类
 */
case class CouponAlertInfo(
                            mid: String,
                            uids: java.util.HashSet[String],
                            itemIds: java.util.HashSet[String],
                            events: java.util.List[String],
                            ts: Long
                          )

