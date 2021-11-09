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

package com.dtstack.flinkx.connector.elasticsearch7.table.lookup;

import com.dtstack.flinkx.connector.elasticsearch7.conf.ElasticsearchConf;
import com.dtstack.flinkx.connector.elasticsearch7.utils.ElasticsearchRequestHelper;
import com.dtstack.flinkx.connector.elasticsearch7.utils.ElasticsearchUtil;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.enums.ECacheContentType;
import com.dtstack.flinkx.lookup.AbstractLruTableFunction;
import com.dtstack.flinkx.lookup.cache.CacheMissVal;
import com.dtstack.flinkx.lookup.cache.CacheObj;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
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
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 13:26
 */
public class ElasticsearchLruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchLruTableFunction.class);
    private final String[] fieldNames;
    private final String[] keyNames;
    private final ElasticsearchConf elasticsearchConf;
    private RestHighLevelClient rhlClient;

    public ElasticsearchLruTableFunction(
            ElasticsearchConf elasticsearchConf,
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
        rhlClient = ElasticsearchUtil.createClient(elasticsearchConf);
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws Exception {
        String cacheKey = buildCacheKey(keys);
        SearchRequest searchRequest = buildSearchRequest(keys);
        rhlClient.searchAsync(
                searchRequest,
                RequestOptions.DEFAULT,
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
     * @param keys
     * @return
     */
    private SearchRequest buildSearchRequest(Object... keys) {
        SearchSourceBuilder sourceBuilder =
                ElasticsearchRequestHelper.createSourceBuilder(fieldNames, keyNames, keys);
        sourceBuilder.size(lookupConf.getFetchSize());
        return ElasticsearchRequestHelper.createSearchRequest(
                elasticsearchConf.getIndex(), null, sourceBuilder);
    }
}
