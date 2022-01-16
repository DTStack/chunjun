/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.elasticsearch6;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @description: build request
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/19 13:59
 */
public class Elasticsearch6RequestFactory {

    /**
     * create Elasticsearch UpdateRequest.
     *
     * @param index
     * @param docType
     * @param key
     * @return
     */
    public static UpdateRequest createUpdateRequest(
            String index, String docType, String key, Map<String, Object> dataMap) {
        return new UpdateRequest(index, docType, key).doc(dataMap).upsert(dataMap);
    }

    /**
     * create Elasticsearch IndexRequest.
     *
     * @param index
     * @param docType
     * @param dataMap
     * @return
     */
    public static IndexRequest createIndexRequest(
            String index, String docType, Map<String, Object> dataMap) {
        return new IndexRequest(index, docType).source(dataMap);
    }

    /**
     * create Elasticsearch DeleteRequest.
     *
     * @param index
     * @param docType
     * @param key
     * @return
     */
    public static DeleteRequest createDeleteRequest(String index, String docType, String key) {
        return new DeleteRequest(index, docType, key);
    }

    /**
     * build search request
     *
     * @param index
     * @param docType
     * @param scroll
     * @param searchSourceBuilder
     * @return
     */
    public static SearchRequest createSearchRequest(
            String index, String docType, Scroll scroll, SearchSourceBuilder searchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(docType);
        if (scroll != null) {
            searchRequest.scroll(scroll);
        }
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    public static SearchSourceBuilder createSourceBuilder(
            String[] fieldsName, String[] keyNames, Object... keys) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        if (keyNames != null && keys != null && keyNames.length > 0) {
            List<String> keyValues =
                    Arrays.stream(keys).map(e -> String.valueOf(e)).collect(Collectors.toList());
            List<String> tempKeyNames = Arrays.asList(keyNames);
            for (int i = 0; i < tempKeyNames.size(); i++) {
                queryBuilder.must(QueryBuilders.termQuery(tempKeyNames.get(i), keyValues.get(i)));
            }
        }
        sourceBuilder.query(queryBuilder);
        sourceBuilder.fetchSource(fieldsName, null);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        return sourceBuilder;
    }
}
