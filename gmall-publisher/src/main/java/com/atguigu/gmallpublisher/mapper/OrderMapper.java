package com.atguigu.gmallpublisher.mapper;

public interface OrderMapper {
    //获取交易额总数抽象方法
    public Double selectOrderAmountTotal(String date);
}
