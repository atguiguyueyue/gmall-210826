package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //返回日活总数数据
    public Integer getDauTotal(String date);

    //返回分时数据
    public Map<String, Long> getDauTotalHour(String date);

    //返回交易额总数数据
    public Double getGmvTotal(String date);
}
