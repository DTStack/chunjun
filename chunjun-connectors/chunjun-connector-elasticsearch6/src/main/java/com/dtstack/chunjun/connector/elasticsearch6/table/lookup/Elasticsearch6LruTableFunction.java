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
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6Conf;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6RequestFactory;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.conf.LookupConf;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @description:
 * @program chunjun
 * @author: lany
 * @create: 2021/06/24 22:47
 */
public class Elasticsearch6LruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = 2L;
    private static Logger LOG = LoggerFactory.getLogger(Elasticsearch6LruTableFunction.class);

    private Elasticsearch6Conf elasticsearchConf;
    private final String[] fieldNames;
    private final String[] keyNames;
    private RestHighLevelClient rhlClient;

    public Elasticsearch6LruTableFunction(
            Elasticsearch6Conf elasticsearchConf,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            AbstractRowConverter rowConverter) {
        super(lookupConf, rowConverter);
        this.elasticsearchConf = elasticsearchConf;
        this.keyNames = keyNames;
        this.fieldNames = fieldNames;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        rhlClient = Elasticsearch6ClientFactory.createClient(elasticsearchConf);
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws Exception {
        String cacheKey = buildCacheKey(keys);
        SearchRequest searchRequest = buildSearchRequest(keys);
        rhlClient.searchAsync(
                searchRequest,
                new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        try {
                            SearchHit[] searchHits = searchResponse.getHits().getHits();
                            if (searchHits.length > 0) {

                                List<Map<String, Object>> cacheContent = Lists.newArrayList();
                                List<RowData> rowList = Lists.newArrayList();
                                for (SearchHit searchHit : searchHits) {
                                    Map<String, Object> result = searchHit.getSourceAsMap();
                                    RowData rowData;
                                    try {
                                        rowData = rowConverter.toInternalLookup(result);
                                        if (openCache()) {
                                            cacheContent.add(result);
                                        }
                                        rowList.add(rowData);
                                    } catch (Exception e) {
                                        LOG.error("error:{} \n  data:{}", e.getMessage(), result);
                                    }
                                }
                                dealCacheData(
                                        cacheKey,
                                        CacheObj.buildCacheObj(
                                                ECacheContentType.MultiLine, cacheContent));

                                future.complete(rowList);
                            } else {
                                dealMissKey(future);
                                dealCacheData(cacheKey, CacheMissVal.getMissKeyObj());
                            }
                        } catch (Exception e) {
                            LOG.error("", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        future.completeExceptionally(new RuntimeException("Response failed!"));
                    }
                });
    }

    /**
     * build search request
     *
     * @return
     */
    private SearchRequest buildSearchRequest(Object... keys) {
        SearchSourceBuilder sourceBuilder =
                Elasticsearch6RequestFactory.createSourceBuilder(fieldNames, keyNames, keys);
        sourceBuilder.size(lookupConf.getFetchSize());
        return Elasticsearch6RequestFactory.createSearchRequest(
                elasticsearchConf.getIndex(), elasticsearchConf.getType(), null, sourceBuilder);
    }
}
