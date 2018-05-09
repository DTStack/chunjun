package com.dtstack.flinkx.es.reader.test;


import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class EsReadDemo {

    public static void searchAll() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("rdos1", 9200, "http"),
                        new HttpHost("rdos2", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest();
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//        searchSourceBuilder().from(0);
//        searchSourceBuilder().size(100);

        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits searchHits = searchResponse.getHits();

        for(SearchHit searchHit : searchHits) {
            Map<String,Object> source = searchHit.getSourceAsMap();
            System.out.println(source);
        }


    }

    public static void searchPart() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("rdos1", 9200, "http"),
                        new HttpHost("rdos2", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.from(0);
        sourceBuilder.size(100);

//        QueryBuilder qb2 = QueryBuilders.wrapperQuery("{\"match_all\": {}}");
        Map<String,Object> map = new HashMap<>();
        Map<String,Object> match = new HashMap<>();
        map.put("match",match);
        match.put("col2", "hallo");
        Gson gson = new Gson();

        //QueryBuilder qb2 = QueryBuilders.wrapperQuery("{\"match\": {\"col2\":\"hallo\"}}");
        QueryBuilder qb2 = QueryBuilders.wrapperQuery(gson.toJson(map));
        sourceBuilder.query(qb2);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        System.out.println(searchResponse);
        SearchHits searchHits = searchResponse.getHits();
        for(SearchHit searchHit : searchHits.getHits()) {
            System.out.println(searchHit.docId() + ":" + searchHit.getSourceAsMap());
        }
        long total = searchHits.getTotalHits();
        System.out.println("total: " + total);

        client.close();
    }


    public static void main(String[] args) throws IOException {
        //searchAll();
        searchPart();
        //searchAll();
    }

}
