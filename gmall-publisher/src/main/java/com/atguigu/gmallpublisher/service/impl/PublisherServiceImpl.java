package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauTotalHour(String date) {
        //1.获取老的Map集合
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建新的Map集合
        HashMap<String, Long> result = new HashMap<>();

        //3.取出老Map中的数据放入新Map集合
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getGmvTotalHour(String date) {
        //1.获取老的Map集合
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建新的Map集合
        HashMap<String, Double> result = new HashMap<>();

        //3.取出老Map中的数据放入新Map集合
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }
}
