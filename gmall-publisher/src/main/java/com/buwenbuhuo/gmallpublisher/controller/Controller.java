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

        // 2.创建list集合用来存放结果数据
        ArrayList<Object> result = new ArrayList<>();

        // 3. 创建map集合用来分别存放新增日活数据和新增设备数据
        HashMap<Object, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        HashMap<Object, Object> devMap = new HashMap<>();
        devMap.put("id","new_mid");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        // 4. 将封装好的map集合存放到list集合中
        result.add(dauMap);
        result.add(devMap);

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
        // 获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        //获取今天日活数据
        Map todayHourMap = publisherService.getDauHourTotal(date);
        //获取昨天数据
        Map yesterdayHourMap = publisherService.getDauHourTotal(yesterday);

        /**
         * 2.创建map集合用于存放结果数据
         */
        HashMap<String, Object> result = new HashMap<>();
        result.put("yesterday", yesterdayHourMap);
        result.put("today", todayHourMap);

        return JSONObject.toJSONString(result);
    }
}
