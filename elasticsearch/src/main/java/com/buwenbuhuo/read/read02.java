package com.buwenbuhuo.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-03-19 1:13
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:elasticsearch的读取数据方法2
 */
public class read02 {
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

        // TODO -------------------------{ }---------------------------------
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // TODO -------------------------bool---------------------------------
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        // TODO -------------------------term---------------------------------
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "male");
        // TODO -------------------------filter---------------------------------
        boolQueryBuilder.filter(termQueryBuilder);
        // TODO -------------------------match---------------------------------
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "穿越火线");
        // TODO -------------------------must---------------------------------
        boolQueryBuilder.must(matchQueryBuilder);
        // TODO -------------------------query---------------------------------
        searchSourceBuilder.query(boolQueryBuilder);
        // TODO -------------------------terms---------------------------------
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByClass")
                .field("class_id");
        // TODO -------------------------max---------------------------------
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByMaxAge")
                .field("age");
        // TODO -------------------------aggs---------------------------------
        searchSourceBuilder.aggregation(termsAggregationBuilder.subAggregation(maxAggregationBuilder));
        // TODO -------------------------from---------------------------------
        searchSourceBuilder.from(0);
        // TODO -------------------------size---------------------------------
        searchSourceBuilder.size(4);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();


        // 6.执行查询
        SearchResult result = jestClient.execute(search);
        // 7.获取命中条数
        System.out.println("命中条数："+result.getTotal());

        // 8.获取数据详情
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
