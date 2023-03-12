package cn.itcast.hotel;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.assertj.core.util.Lists;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
@Slf4j
public class YHotelDocumentSearchTest {

    @Autowired
    private RestHighLevelClient client;
    @Test
    public void searchAllTest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source()
                .query(QueryBuilders.matchAllQuery())
                ;
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
    }

    @Test
    public void searchMatchTest() throws IOException{
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source()
                .query(QueryBuilders.matchQuery("all","如家"));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
    }

    @Test
    public void searchMulitQueryTest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source()
                .query(QueryBuilders.multiMatchQuery("如家", "brand","name"));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
    }

    @Test
    public void searchTermTest() throws IOException {
        SearchRequest searchRequest=new SearchRequest("hotel");
        searchRequest.source()
                .query(QueryBuilders.termQuery("business","四川北路商业区"));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
    }

    @Test
    public void searchRangTest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source()
                .query(QueryBuilders.rangeQuery("price").gte("100").lte("20000"));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
    }

    @Test
    public void searchDistance() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source()
                .query(QueryBuilders.geoDistanceQuery("distance").point(31.21d,121.5d).distance(2, DistanceUnit.KILOMETERS));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
    }

    @Test
    public void searchFunction(){
        SearchRequest searchRequest = new SearchRequest("hotel");
//        searchRequest.source()
//                .query(QueryBuilders.functionScoreQuery(Sc))
    }

    @Test
    public void searchBoolean() throws IOException {
        SearchRequest searchRequest=new SearchRequest("hotel");
        searchRequest.source()
                .query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name", "如家")))
                .from(0)
                .size(20);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
    }

    @Test
    void searchHighLight() throws IOException {
        SearchRequest searchRequest=new SearchRequest("hotel");
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        List<HighlightBuilder.Field> fields = highlightBuilder.fields();
        HighlightBuilder.Field field1=new HighlightBuilder.Field("name");
        HighlightBuilder.Field field2=new HighlightBuilder.Field("brand");
        HighlightBuilder.Field field3=new HighlightBuilder.Field("address");
        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        highlightBuilder.requireFieldMatch(false);
        searchRequest.source()
                .query(QueryBuilders.matchQuery("all","如家"))
                .highlighter(highlightBuilder);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
    }

    @Test
    void bucket() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.boolQuery().filter(QueryBuilders.matchAllQuery()));
        searchRequest.source().aggregation(AggregationBuilders.terms("brand_agg").field("brand")
                .subAggregation(AggregationBuilders.stats("score_agg").field("score"))
                .order(BucketOrder.aggregation("score_agg.avg",false)).size(10));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = search.getAggregations();
        Terms brand_agg = aggregations.get("brand_agg");
        List<? extends Terms.Bucket> buckets = brand_agg.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            Aggregations aggregations1 = bucket.getAggregations();
            Stats score_agg = aggregations1.get("score_agg");
            double avg = score_agg.getAvg();
            System.out.println(score_agg.getName()+":"+avg);
        }
        System.out.println("===");
    }

    @BeforeEach
    public void build(){
        client=new RestHighLevelClient(RestClient.builder(HttpHost.create("127.0.0.1:9200")));
    }

    @AfterEach
    public void distory() throws IOException {
        client.close();
    }
}
