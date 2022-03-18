package com.buwenbuhuo.bean

/**
 * Author 不温卜火
 * Create 2022-03-18 11:36
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: OrderInfo样例类
 */
case class OrderInfo(
                      id: String,
                      province_id: String,
                      consignee: String,
                      order_comment: String,
                      var consignee_tel: String,
                      order_status: String,
                      payment_way: String,
                      user_id: String,
                      img_url: String,
                      total_amount: Double,
                      expire_time: String,
                      delivery_address: String,
                      create_time: String,
                      operate_time: String,
                      tracking_no: String,
                      parent_order_id: String,
                      out_trade_no: String,
                      trade_body: String,
                      var create_date: String,
                      var create_hour: String
                    )

