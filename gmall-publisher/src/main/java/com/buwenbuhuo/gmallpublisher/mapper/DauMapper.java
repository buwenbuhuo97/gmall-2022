package com.buwenbuhuo.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-03-17 13:11
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 操作数据
 */
public interface DauMapper {
    // 获取日活总数数据
    public Integer selectDauTotal(String date);

    // 获取日活分时数据
    public List<Map> selectDauTotalHourMap(String date);
}
