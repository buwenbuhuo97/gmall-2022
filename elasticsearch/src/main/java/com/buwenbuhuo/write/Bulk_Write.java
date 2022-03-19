package com.buwenbuhuo.write;

import com.buwenbuhuo.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import java.io.IOException;

/**
 * Author 不温卜火
 * Create 2022-03-19 1:05
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:elasticsearch的批量写入数据示例demo
 */
public class Bulk_Write {
    public static void main(String[] args) throws IOException {
        // 1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        // 2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig
                .Builder("http://hadoop01:9200")
                .build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // 3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        // 4. 批量写入数据
        Movie movie1 = new Movie("004", "长津湖");
        Movie movie2 = new Movie("005", "误杀");
        Movie movie3 = new Movie("006", "红海行动");
        Index index1 = new Index.Builder(movie1).id("1004").build();
        Index index2 = new Index.Builder(movie2).id("1005").build();
        Index index3 = new Index.Builder(movie3).id("1006").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie1")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .addAction(index3)
                .build();
        jestClient.execute(bulk);
        jestClient.shutdownClient();
    }
}
