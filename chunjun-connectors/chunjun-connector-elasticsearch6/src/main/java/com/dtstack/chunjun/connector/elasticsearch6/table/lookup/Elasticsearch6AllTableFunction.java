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

package com.dtstack.chunjun.connector.elasticsearch6.table.lookup;

import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6ClientFactory;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6Config;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6RequestFactory;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.GenericRowData;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Elasticsearch6AllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = -2046161755223639032L;

    private final Elasticsearch6Config elasticsearchConfig;

    public Elasticsearch6AllTableFunction(
            Elasticsearch6Config elasticsearchConfig,
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

        SearchRequest requestBuilder = buildSearchRequest();

        SearchResponse searchResponse;
        SearchHit[] searchHits;
        try (RestHighLevelClient rhlClient =
                Elasticsearch6ClientFactory.createClient(elasticsearchConfig)) {
            searchResponse = rhlClient.search(requestBuilder);
            searchHits = searchResponse.getHits().getHits();
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
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private SearchRequest buildSearchRequest() {
        SearchSourceBuilder sourceBuilder =
                Elasticsearch6RequestFactory.createSourceBuilder(fieldsName, null, null);
        sourceBuilder.size(lookupConfig.getFetchSize());
        return Elasticsearch6RequestFactory.createSearchRequest(
                elasticsearchConfig.getIndex(), elasticsearchConfig.getType(), null, sourceBuilder);
    }
}
