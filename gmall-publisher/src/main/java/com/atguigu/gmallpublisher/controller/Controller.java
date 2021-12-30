package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {

        //1.获取日活总数
        Integer dauTotal = publisherService.getDauTotal(date);

        //2.创建存放新增日活的Map集合
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //3.创建存放新增设备的Map集合
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        //4.将Map集合封装到List集合中
        ArrayList<Map> result = new ArrayList<>();
        result.add(dauMap);
        result.add(devMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHour(
            @RequestParam("id") String id,
            @RequestParam("date") String date){

        //1.根据今天的日期获取到昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //2.根据日期获取Service处理后的数据
        Map<String, Long> todayMap = publisherService.getDauTotalHour(date);

        Map<String, Long> yesterdayMap = publisherService.getDauTotalHour(yesterday);

        //3.创建存放最终结果的Map集合
        HashMap<String, Map> result = new HashMap<>();

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }
}
