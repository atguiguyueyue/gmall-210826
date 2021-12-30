package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
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
}
