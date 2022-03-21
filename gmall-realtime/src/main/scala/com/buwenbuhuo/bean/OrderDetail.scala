package com.buwenbuhuo.bean

/**
 * Author 不温卜火
 * Create 2022-03-20 0:30
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */
case class OrderDetail(
                        id: String,
                        order_id: String,
                        sku_name: String,
                        sku_id: String,
                        order_price: String,
                        img_url: String,
                        sku_num: String
                      )
