package com.dtstack.flinkx.es.writer.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EsDemo {

    public static void test1() throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("rdos1", 9200, "http"),
                        new HttpHost("rdos2", 9200, "http")));

        IndexRequest request = new IndexRequest(
                "nani222",
                "doc222",
                "id2");

        String jsonString = "{" +
                "\"user\":\"user2\"," +
                "\"postDate\":\"2014-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";


        request.source(jsonString, XContentType.JSON);
        IndexResponse response = client.index(request);
        System.out.println(response.getResult());
        client.close();
    }

    public static void test3() throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("study", 9200, "http"),
                        new HttpHost("study", 9201, "http")));

        IndexRequest request = new IndexRequest(
                "nani",
                "doc");

//        String jsonString = "{" +
//                "\"user\":\"xxxx\"," +
//                "\"postDate\":\"2013-01-30\"," +
//                "\"message\":\"trying out Elasticsearch\"" +
//                "}";
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("xxx", "asfdasdf");
        jsonMap.put("zzz", "asdfsadf");
        request.source(jsonMap);
        IndexResponse response = client.index(request);
        System.out.println(response.getResult());
        client.close();
    }

    public static void test2() throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        UpdateRequest request = new UpdateRequest(
                "nani250",
                "doc",
                "2");

        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        request.doc(jsonString, XContentType.JSON);
        UpdateResponse response = client.update(request);
        System.out.println(response.getResult());
        client.close();
    }

    public static void test4() throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("study", 9200, "http"),
                        new HttpHost("study", 9201, "http")));
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        SearchResponse searchResponse = client.search(searchRequest);
        System.out.println(searchResponse.getTotalShards());
    }

    public static void test5() throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("study", 9200, "http"),
                        new HttpHost("study", 9201, "http")));
        BulkRequest bulkRequest = new BulkRequest();

        IndexRequest request = new IndexRequest("nani", "doc1");
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("xxx", "8888");
        jsonMap.put("yyy", "9999");

        bulkRequest.add(request.source(jsonMap));
        // bulkRequest.setRefreshPolicy(null);
        // WriteRequest.RefreshPolicy;

        BulkResponse bulkResponse = client.bulk(bulkRequest);

        System.out.println(bulkResponse);
    }

    public static void test6() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("study", 9200, "http"),
                        new HttpHost("study", 9201, "http")));
        SearchRequest searchRequest = new SearchRequest();
        SearchResponse resp =  client.search(searchRequest);
        resp.getAggregations();
    }

    public static void main(String[] args) throws Exception {
        test1();
    }


}
