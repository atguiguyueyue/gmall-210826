package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class ES02_BulkWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.通过客户端工厂获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.批量写入数据
        Movie movie103 = new Movie("103", "特斯拉大战金刚");
        Movie movie104 = new Movie("104", "柯南：绯色子弹");
        Movie movie105 = new Movie("105", "我和我的父辈");

        Index index103 = new Index.Builder(movie103).id("1003").build();
        Index index104 = new Index.Builder(movie104).id("1004").build();
        Index index105 = new Index.Builder(movie105).id("1005").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie")
                .defaultType("_doc")
                .addAction(index103)
                .addAction(index104)
                .addAction(index105)
                .build();

        jestClient.execute(bulk);
        //关闭连接
        jestClient.shutdownClient();
    }
}
