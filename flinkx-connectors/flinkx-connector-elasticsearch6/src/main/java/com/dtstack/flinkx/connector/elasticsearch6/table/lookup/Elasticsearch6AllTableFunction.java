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

package com.dtstack.flinkx.connector.elasticsearch6.table.lookup;

import com.dtstack.flinkx.connector.elasticsearch6.Elasticsearch6ClientFactory;
import com.dtstack.flinkx.connector.elasticsearch6.Elasticsearch6Conf;
import com.dtstack.flinkx.connector.elasticsearch6.Elasticsearch6RequestFactory;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.table.data.GenericRowData;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/24 22:47
 */
public class Elasticsearch6AllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 2L;
    private static Logger LOG = LoggerFactory.getLogger(Elasticsearch6LruTableFunction.class);

    private final Elasticsearch6Conf elasticsearchConf;
    private transient RestHighLevelClient rhlClient;

    public Elasticsearch6AllTableFunction(
            Elasticsearch6Conf elasticsearchConf,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            AbstractRowConverter rowConverter) {
        super(fieldNames, keyNames, lookupConf, rowConverter);
        this.elasticsearchConf = elasticsearchConf;
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;

        rhlClient = Elasticsearch6ClientFactory.createClient(elasticsearchConf);
        SearchRequest requestBuilder = buildSearchRequest();

        SearchResponse searchResponse;
        SearchHit[] searchHits;
        try {
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
                    LOG.error("error:{} \n  data:{}", e.getMessage(), source);
                }
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    /**
     * build search request
     *
     * @return
     */
    private SearchRequest buildSearchRequest() {
        SearchSourceBuilder sourceBuilder =
                Elasticsearch6RequestFactory.createSourceBuilder(fieldsName, null, null);
        sourceBuilder.size(lookupConf.getFetchSize());
        return Elasticsearch6RequestFactory.createSearchRequest(
                elasticsearchConf.getIndex(), elasticsearchConf.getType(), null, sourceBuilder);
    }
}
