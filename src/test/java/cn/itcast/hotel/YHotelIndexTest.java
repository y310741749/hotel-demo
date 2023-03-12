package cn.itcast.hotel;

import cn.itcast.hotel.constants.HotelIndexConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class YHotelIndexTest {

    private RestHighLevelClient restHighLevelClient;
    @BeforeEach
    public void build(){
        restHighLevelClient=new RestHighLevelClient(RestClient.builder(HttpHost.create("127.0.0.1:9200")));
    }

    @AfterEach
    public void distory() throws IOException {
        restHighLevelClient.close();
    }

    @Test
    public void test1(){
        System.out.println(restHighLevelClient);
    }

    @Test
    public void createIndexHotel() throws IOException {
        CreateIndexRequest createIndexRequest=new CreateIndexRequest("hotel");
        createIndexRequest.source(HotelIndexConstants.MAPPING_TEMPLATE, XContentType.JSON);
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void isExits() throws IOException {
        GetIndexRequest getIndexRequest=new GetIndexRequest("hotel");
        boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    public void delete() throws IOException {
        GetIndexRequest getIndexRequest=new GetIndexRequest("hotel");
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        DeleteIndexRequest deleteIndexRequest=new DeleteIndexRequest("hotel");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);


        boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);

    }
}
