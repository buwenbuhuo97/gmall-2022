package com.buwenbuhuo.gmallpublisher.service.impl;

import com.buwenbuhuo.constants.GmallConstants;
import com.buwenbuhuo.gmallpublisher.bean.Option;
import com.buwenbuhuo.gmallpublisher.bean.Stat;
import com.buwenbuhuo.gmallpublisher.mapper.DauMapper;
import com.buwenbuhuo.gmallpublisher.mapper.OrderMapper;
import com.buwenbuhuo.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-03-17 13:24
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 获取数据的具体实现
 */

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired  // 自动注入
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourTotal(String date) {
        // 1. 获取Mapper层的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        // 2.创建新的map集合用来存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        // 3.遍历list集合提取每个map
        for (Map map : list) {
            result.put((String) map.get("LH"),(Long) map.get("CT"));
        }

        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getGmvHourTotal(String date) {
        // 1.获取mapper层查询的数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        // 2.创建map集合用来存放结果数据
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map getSaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_QUERY_INDEXNAME).addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        // TODO 1.获取总数
        Long total = searchResult.getTotal();

        // TODO 2.获取数据明细
        // 创建一个存放明细数据的List集合
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }

        // TODO 3.获取聚合组数据
        // 创建list集合用来存放年龄的Option对象
        ArrayList<Option> ageOptions = new ArrayList<>();
        // 创建list集合用来存放年龄的Option对象
        ArrayList<Option> genderOptions = new ArrayList<>();
        // 1.获取年龄占比数据
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation groupbyAge = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets = groupbyAge.getBuckets();
        // 存放20岁以下的人数
        Long low20Count = 0L;
        // 存放30岁及30岁以上的人数
        Long up30Count = 0L;
        for (TermsAggregation.Entry bucket : buckets) {
            // 获取20岁以下的个数
            if (Integer.parseInt(bucket.getKey()) < 20) {
                low20Count += bucket.getCount();
             // 获取30岁及30岁以上的个数
            }else if(Integer.parseInt(bucket.getKey())>30){
                up30Count += bucket.getCount();
            }
        }
        // 获取小于20岁的年龄占比
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;

        // 获取大于30岁的年龄占比
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;

        // 获取大于20小于30的年龄占比
        double up20AndLow30Ratio = Math.round((100D - low20Ratio - up30Ratio) * 10D) / 10D;
        Option low20Opt = new Option("20岁以下", low20Ratio);
        Option up30Opt = new Option("30岁及30岁以上", up30Ratio);
        Option up20AndLow30Opt = new Option("20岁到30岁", up20AndLow30Ratio);
        ageOptions.add(low20Opt);
        ageOptions.add(up30Opt);
        ageOptions.add(up20AndLow30Opt);
        // 创建年龄占比的Stat对象
        Stat ageStat = new Stat(ageOptions, "用户年龄占比");

        // 2.获取用户性别占比数据
        MetricAggregation aggregations1 = searchResult.getAggregations();
        TermsAggregation groupbyGender = aggregations1.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> buckets1 = groupbyGender.getBuckets();
        // 定义一个变量用来存放男生个数
        Long male = 0L;
        for (TermsAggregation.Entry entry : buckets1) {
            if ("M".equals(entry.getKey())){
                male += entry.getCount();
            }
        }

        // 男性占比
        double maleRatio = Math.round(male * 1000D / total) / 10D;

        // 女性占比
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);
        Stat genderStat = new Stat(genderOptions, "用户性别占比");

        // 创建List集合用来存放Stat对象
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        // 创建Map集合用来存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat",stats);
        result.put("detail",detail);

        return result;

    }
}

