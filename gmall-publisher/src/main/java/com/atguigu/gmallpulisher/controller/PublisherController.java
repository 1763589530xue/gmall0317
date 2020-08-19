package com.atguigu.gmallpulisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpulisher.service.PublisherService;
import org.apache.ibatis.executor.ResultExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.*;

@RestController
public class PublisherController {


    @Autowired
    private PublisherService publisherService;  //本质创建的是service层的PublisherServiceImpl对象；


    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {

        //1.查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

        //2.创建List用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建Map用于存放日活数据
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //4.创建Map用于存放新增用户数据
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //5.创建Map用于存放新增用户数据
        HashMap<String, Object> orderMap = new HashMap<>();
        orderMap.put("id", "order_amount");
        orderMap.put("name", "新增交易额");
        orderMap.put("value", orderAmountTotal);

        //6.将Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(orderMap);

        //7.返回结果
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date) {

        //创建Map用于存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        if ("dau".equals(id)) {

            //1.查询当天的分时数据
            Map todayMap = publisherService.getDauTotalHourMap(date);

            //2.查询昨天的分时数据
            // java1.8后增加地LocalDate/LocalTime/LocalDateTime用来代替之前的Date/Calader等类；
            String yesterday = LocalDate.parse(date).plusDays(-1).toString();
            Map yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

            //3.将yesterdayMap和todayMap放入result
            result.put("yesterday", yesterdayMap);
            result.put("today", todayMap);

        } else if ("new_mid".equals(id)) {

            HashMap<String, Long> yesterdayMap = new HashMap<>();
            yesterdayMap.put("09", 100L);
            yesterdayMap.put("12", 200L);
            yesterdayMap.put("17", 150L);

            HashMap<String, Long> todayMap = new HashMap<>();
            todayMap.put("10", 400L);
            todayMap.put("13", 450L);
            todayMap.put("15", 500L);
            todayMap.put("20", 600L);

            result.put("yesterday", yesterdayMap);
            result.put("today", todayMap);
        }else if ("order_amount".equals(id)) {

            //1.查询当天的分时数据
            Map todayMap = publisherService.getOrderAmountHourMap(date);

            //2.查询昨天的分时数据
            String yesterday = LocalDate.parse(date).plusDays(-1).toString();
            Map yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);

            //3.将yesterdayMap和todayMap放入result
            result.put("yesterday", yesterdayMap);
            result.put("today", todayMap);
        }

        return JSONObject.toJSONString(result);
    }
}
