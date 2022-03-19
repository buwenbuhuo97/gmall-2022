package com.buwenbuhuo.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-03-17 13:27
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 接受请求层
 */

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    /**
     * 封装总数数据
     * @param date
     * @return
     */
    @RequestMapping("realtime-total")
    public String getDapTotal(@RequestParam("date") String date){
        // 1.从service层获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);

        // 2.获取交易额总数数据
        Double gmvTotal = publisherService.getGmvTotal(date);

        // 3.创建list集合用来存放结果数据
        ArrayList<Object> result = new ArrayList<>();

        // 4. 创建map集合用来分别存放新增日活数据和新增设备数据
        HashMap<Object, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        HashMap<Object, Object> devMap = new HashMap<>();
        devMap.put("id","new_mid");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        // 5. 创建存放交易额总数的map集合
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", gmvTotal);


        // 6. 将封装好的map集合存放到list集合中
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    /**
     * 封装分时数据
     * @param id
     * @param date
     * @return
     */
    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date) {
        /**
         * 获取Service层的数据
         */
        //  获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();


        // TODO 根据id进行判断，往最终map集合中放的是哪个需求的数据
        Map todayHourMap = null;
        Map yesterdayHourMap = null;
        if ("order_amount".equals(id)){
            // 分别获取昨天和今天的数据
            todayHourMap = publisherService.getGmvHourTotal(date);
            yesterdayHourMap = publisherService.getGmvHourTotal(yesterday);
        }else if ("dau".equals(id)){
            //获取今天日活数据
            todayHourMap = publisherService.getDauHourTotal(date);
            //获取昨天数据
            yesterdayHourMap = publisherService.getDauHourTotal(yesterday);
        }

        /**
         * 2.创建map集合用于存放结果数据
         */
        HashMap<String, Object> result = new HashMap<>();
        result.put("yesterday", yesterdayHourMap);
        result.put("today", todayHourMap);

        return JSONObject.toJSONString(result);
    }
}
