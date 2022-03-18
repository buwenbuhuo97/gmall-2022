package com.buwenbuhuo.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-03-18 12:40
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 查询当日交易额总数和分时明细
 */
public interface OrderMapper {

    //1 查询当日交易额总数
    public  Double selectOrderAmountTotal(String date);

    //2 查询当日交易额分时明细
    public  List<Map> selectOrderAmountHourMap(String date);

}

