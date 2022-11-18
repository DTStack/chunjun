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

package com.dtstack.chunjun.connector.elasticsearch7.table.lookup;

import com.dtstack.chunjun.connector.elasticsearch7.Elasticsearch7ClientFactory;
import com.dtstack.chunjun.connector.elasticsearch7.Elasticsearch7RequestFactory;
import com.dtstack.chunjun.connector.elasticsearch7.ElasticsearchConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.table.data.GenericRowData;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ElasticsearchAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = -1359021097622405548L;

    private final ElasticsearchConfig elasticsearchConfig;
    private transient RestHighLevelClient rhlClient;
    private static final String SORT_COLUMN = "_id";

    public ElasticsearchAllTableFunction(
            ElasticsearchConfig elasticsearchConfig,
            LookupConfig lookupConfig,
            String[] fieldNames,
            String[] keyNames,
            AbstractRowConverter rowConverter) {
        super(fieldNames, keyNames, lookupConfig, rowConverter);
        this.elasticsearchConfig = elasticsearchConfig;
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;

        rhlClient = Elasticsearch7ClientFactory.createClient(elasticsearchConfig, null);
        SearchRequest requestBuilder = buildSearchRequest(null);
        SearchResponse searchResponse;
        SearchHit[] searchHits;
        try {
            searchResponse = rhlClient.search(requestBuilder, RequestOptions.DEFAULT);
            searchHits = searchResponse.getHits().getHits();
            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit searchHit : searchHits) {
                    Map<String, Object> oneRow = new HashMap<>();
                    Map<String, Object> source = searchHit.getSourceAsMap();
                    try {
                        GenericRowData rowData = (GenericRowData) rowConverter.toInternal(source);
                        for (int i = 0; i < fieldsName.length; i++) {
                            Object object = rowData.getField(i);
                            oneRow.put(fieldsName[i].trim(), object);
                        }
                        buildCache(oneRow, tmpCache);
                    } catch (Exception e) {
                        log.error("error:{} \n  data:{}", e.getMessage(), source);
                    }
                }
                requestBuilder =
                        buildSearchRequest(searchHits[searchHits.length - 1].getSortValues());
                searchResponse = rhlClient.search(requestBuilder, RequestOptions.DEFAULT);
                searchHits = searchResponse.getHits().getHits();
            }

        } catch (IOException e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    private SearchRequest buildSearchRequest(Object[] searchAfter) {
        SearchSourceBuilder sourceBuilder =
                Elasticsearch7RequestFactory.createSourceBuilder(fieldsName, null, null);
        sourceBuilder.size(lookupConfig.getFetchSize()).sort(SORT_COLUMN);
        if (searchAfter != null) {
            sourceBuilder.searchAfter(searchAfter);
        }
        return Elasticsearch7RequestFactory.createSearchRequest(
                elasticsearchConfig.getIndex(), null, sourceBuilder);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (rhlClient != null) {
            rhlClient.close();
        }
    }
}
