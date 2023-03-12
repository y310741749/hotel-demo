package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

@SpringBootTest
public class YHotrlDocumentTest {
    private RestHighLevelClient restHighLevelClient;
    @Autowired
    private HotelService hotelService;

    @BeforeEach
    public void build() {
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(HttpHost.create("127.0.0.1:9200")));
    }

    @AfterEach
    public void distory() throws IOException {
        restHighLevelClient.close();
    }

    @Test
    public void insertOne() throws IOException {
        Hotel hotel = hotelService.getById(36934);
        IndexRequest indexRequest = new IndexRequest("hotel1").id(hotel.getId().toString());
        indexRequest.source(JSON.toJSONString(new HotelDoc(hotel)), XContentType.JSON);
        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void get() throws IOException {
        GetRequest getRequest = new GetRequest("hotel").id("38609");
        GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
//        MultiGetRequest multiGetRequest = new MultiGetRequest();
//        MultiGetResponse mget = restHighLevelClient.mget(multiGetRequest, RequestOptions.DEFAULT);
        System.out.println(documentFields);
    }

    @Test
    public void update() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("hotel", "36934");
        HashMap<String, Object> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put("id", 1);
        UpdateRequest doc = updateRequest.doc(objectObjectHashMap);
        UpdateResponse update = restHighLevelClient.update(doc, RequestOptions.DEFAULT);
    }

    @Test
    public void delete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("hotel").id("1");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    @Test
    public void Bulk() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        List<Hotel> list = hotelService.list();
        for (Hotel hotel : list) {
            IndexRequest indexRequest = new IndexRequest("hotel").id(hotel.getId().toString());
            indexRequest.source(JSON.toJSONString(new HotelDoc(hotel)), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    void SuggestionSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().suggest(new SuggestBuilder().
                addSuggestion("suggestions",
                        SuggestBuilders.completionSuggestion("suggestion").prefix("sz").skipDuplicates(true).size(10)));
        restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
    }
}
