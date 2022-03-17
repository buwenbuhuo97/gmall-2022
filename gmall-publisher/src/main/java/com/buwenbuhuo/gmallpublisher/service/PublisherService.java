package com.buwenbuhuo.gmallpublisher.service;

import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-03-17 13:22
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 获取数据
 */
public interface PublisherService {
    // 获取日活总数数据
    public Integer getDauTotal(String date);

    // 获取日活分时数据
    public Map getDauHourTotal(String date);

}
