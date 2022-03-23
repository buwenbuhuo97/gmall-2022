package com.buwenbuhuo.write;

import com.buwenbuhuo.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import java.io.IOException;
import java.util.HashMap;

/**
 * Author 不温卜火
 * Create 2022-03-19 0:27
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:elasticsearch的单条数据写入示例demo
 */
public class Single_Write {
    public static void main(String[] args) throws IOException {
        // 1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        /**
         * HttpClientConfig后为什么为Builder？
         *  深追源码：
         *      点击进入HttpClientConfig源码，找到Builder的类的build方法，
         *      找到最后，发现其返回Builder对象
         */
        // 2. 设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder
                ("http://hadoop01:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // 3. 获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        // 4. 将单跳数据写入ES
        Movie movie = new Movie("003", "变形金刚");
        Index index = new Index.Builder(movie)
                .index("movie1")
                .type("_doc")
                .id("1003")
                .build();
        jestClient.execute(index);

        // 关闭连接
        jestClient.shutdownClient();
    }
}
