package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.Param;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private RestHighLevelClient client;

    @PostMapping("/list")
    public PageResult hotelDoc(@RequestBody Param param) throws IOException {
        List<HotelDoc> result = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest("hotel");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (!StringUtils.isEmpty(param.getKey())) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("all", param.getKey()));
        } else {
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        }
        if (!StringUtils.isEmpty(param.getCity())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("city", param.getCity()));
        }
        if (!StringUtils.isEmpty(param.getStartName())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("startName", param.getStartName()));
        }
        if (!StringUtils.isEmpty(param.getBrand())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("brand", param.getBrand()));
        }
        if (null != param.getMinPrice()) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(param.getMinPrice()));
        }
        if (null != param.getMaxPrice()) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lte(param.getMaxPrice()));
        }
        FunctionScoreQueryBuilder isAdd = QueryBuilders.functionScoreQuery(
                boolQueryBuilder,
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                QueryBuilders.termQuery("isAD", true),
                                ScoreFunctionBuilders.weightFactorFunction(500)
                        )
                }
        );
        searchRequest.source()
                .query(isAdd)
//                .sort(SortBuilders.geoDistanceSort("location", new GeoPoint("31.21,121.5"))
//                        .order(SortOrder.ASC)
//                        .unit(DistanceUnit.KILOMETERS))
                .from((param.getPage() - 1) * param.getSize())
                .size(param.getSize())
                .highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));

        if (!"default".equals(param.getSortBy())) {
            searchRequest.source()
                    .sort(param.getSortBy(), SortOrder.ASC);
        }
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long total = hits.getTotalHits().value;
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            HighlightField name = highlightFields.get("name");
            String sourceAsString = documentFields.getSourceAsString();
            JSONObject jsonObject = JSON.parseObject(sourceAsString);
            HotelDoc hotelDoc = JSONObject.toJavaObject(jsonObject, HotelDoc.class);
            if (Objects.nonNull(name)) {
                hotelDoc.setName(name.toString());
            }
            Object[] sortValues = documentFields.getSortValues();
            if (sortValues != null && sortValues.length > 0) {
                hotelDoc.setDistance(sortValues[0]);
            }
            result.add(hotelDoc);
        }
        return PageResult.result(total, result);
    }

    @PostMapping("/filters")
    public Map<String,List<String>> filters(@RequestBody Param param) throws IOException {
        Map<String,List<String>> result = new HashMap<>();
        SearchRequest searchRequest=new SearchRequest("hotel");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (!StringUtils.isEmpty(param.getKey())) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("all", param.getKey()));
        } else {
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        }
        if (!StringUtils.isEmpty(param.getCity())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("city", param.getCity()));
        }
        if (!StringUtils.isEmpty(param.getStartName())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("startName", param.getStartName()));
        }
        if (!StringUtils.isEmpty(param.getBrand())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("brand", param.getBrand()));
        }
        if (null != param.getMinPrice()) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(param.getMinPrice()));
        }
        if (null != param.getMaxPrice()) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lte(param.getMaxPrice()));
        }
        searchRequest.source().query(boolQueryBuilder).size(2000);
        searchRequest.source().aggregation(AggregationBuilders.terms("cityAgg").field("city").size(20));
        searchRequest.source().aggregation(AggregationBuilders.terms("brandAgg").field("brand").size(20));
//        searchRequest.source().aggregation(AggregationBuilders.terms("starNameAgg").field("starName"));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = search.getAggregations();
        setResult("cityAgg",aggregations,"city",result);
        setResult("brandAgg",aggregations,"brand",result);
        setResult("starNameAgg",aggregations,"starName",result);
        return result;
    }

    private void setResult(String agg, Aggregations aggregations, String key,Map<String,List<String>> result) {
        Terms aggTerms = aggregations.get(agg);
        if(Objects.isNull(aggTerms)){
            result.put(key,new ArrayList<>());
            return;
        }
        List<? extends Terms.Bucket> buckets = aggTerms.getBuckets();
        List<String> values = buckets.stream().map(p -> ((Terms.Bucket) p).getKeyAsString()).collect(Collectors.toList());
        result.put(key,values);
    }

    @GetMapping("/suggestion")
    public Set<String> suggestion(String key) throws IOException {
        Set<String> result = Sets.newHashSet();
        SearchRequest searchRequest =new SearchRequest("hotel");
        searchRequest.source().suggest(new SuggestBuilder()
        .addSuggestion("mySuggestion", SuggestBuilders.completionSuggestion("suggestion")
        .prefix(key)
        .skipDuplicates(true)
        .size(10)));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        Suggest suggest = search.getSuggest();
        Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> mySuggestion = suggest.getSuggestion("mySuggestion");
        for (Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option> options : mySuggestion.getEntries()) {
            List<? extends Suggest.Suggestion.Entry.Option> options1 = options.getOptions();
            for (Suggest.Suggestion.Entry.Option option : options1) {
                result.add(option.getText().string());
            }
        }
        return result;
    }
}
