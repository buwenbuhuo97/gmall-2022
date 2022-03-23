package com.buwenbuhuo.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-03-19 1:13
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:elasticsearch的读取数据方法1
 */
public class read01 {
    public static void main(String[] args) throws IOException {
        // 1.创建ES客户端连接池
        JestClientFactory factory = new JestClientFactory();

        // 2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder
                ("http://hadoop01:9200").build();
        factory.setHttpClientConfig(httpClientConfig);

        // 3.获取ES客户端连接
        JestClient jestClient = factory.getObject();

        // 4. 查询ES中的数据
        Search search = new Search.Builder("{\n" +
                "   \"query\": {\n" +
                "    \"bool\":{\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"male\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"穿越火线\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupByClass\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\",\n" +
                "        \"size\": 10\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"groupByMaxAge\": {\n" +
                "          \"max\": {\n" +
                "            \"field\": \"age\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 4\n" +
                " \n" +
                "}")
                .addIndex("student")
                .addType("_doc")
                .build();


        // 6.执行查询
        SearchResult result = jestClient.execute(search);
        // 7.获取命中条数
        System.out.println("命中条数："+result.getTotal());

        // 8.获取数据详情
        // Map.class是用来接收明细数据的
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:" + hit.index);
            System.out.println("_type:" + hit.type);
            System.out.println("_id:" + hit.id);
            // 获取数据明细
            Map source = hit.source;
            // 遍历map集合
            for (Object o : source.keySet()) {
                System.out.println(o+":"+source.get(o));
            }
        }
        // 9.获取嵌套聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        // 获取同伴级下的聚合组数据
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:"+bucket.getKey());
            System.out.println("doc_count:"+bucket.getCount());
            //
            MaxAggregation groupByMaxAge = bucket.getMaxAggregation("groupByMaxAge");
            System.out.println("年龄最大值:"+groupByMaxAge.getMax());
        }

        //10.关闭连接
        jestClient.shutdownClient();
    }
}
