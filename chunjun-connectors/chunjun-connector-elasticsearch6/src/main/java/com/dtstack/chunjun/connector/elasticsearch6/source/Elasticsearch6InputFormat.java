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

package com.dtstack.chunjun.connector.elasticsearch6.source;

import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6ClientFactory;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6Config;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6RequestFactory;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class Elasticsearch6InputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = 389258823998116952L;

    /** Elasticsearch Configuration */
    private Elasticsearch6Config elasticsearchConfig;

    /** Elasticsearch High Level Client */
    private transient RestHighLevelClient rhlClient;

    protected String query;

    protected long keepAlive = 1;

    private Iterator<Map<String, Object>> iterator;

    private transient SearchRequest searchRequest;

    private transient Scroll scroll;

    private String scrollId;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }

        return splits;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        super.openInputFormat();
        GenericInputSplit genericInputSplit = (GenericInputSplit) inputSplit;

        rhlClient = Elasticsearch6ClientFactory.createClient(elasticsearchConfig);
        scroll = new Scroll(TimeValue.timeValueMinutes(keepAlive));
        String[] fieldsNames = elasticsearchConfig.getFieldNames();
        SearchSourceBuilder searchSourceBuilder =
                Elasticsearch6RequestFactory.createSourceBuilder(fieldsNames, null, null);
        searchSourceBuilder.size(elasticsearchConfig.getBatchSize());

        if (StringUtils.isNotEmpty(query)) {
            searchSourceBuilder.query(QueryBuilders.wrapperQuery(query));
        }

        if (genericInputSplit.getTotalNumberOfSplits() > 1) {
            searchSourceBuilder.slice(
                    new SliceBuilder(
                            genericInputSplit.getSplitNumber(),
                            genericInputSplit.getTotalNumberOfSplits()));
        }

        searchRequest =
                Elasticsearch6RequestFactory.createSearchRequest(
                        elasticsearchConfig.getIndex(),
                        elasticsearchConfig.getType(),
                        scroll,
                        searchSourceBuilder);
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            rowData = rowConverter.toInternal(iterator.next());
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
        return rowData;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (rhlClient != null) {
            clearScroll();

            rhlClient.close();
            rhlClient = null;
        }
    }

    private void clearScroll() throws IOException {
        if (scrollId == null) {
            return;
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = rhlClient.clearScroll(clearScrollRequest);
        boolean succeeded = clearScrollResponse.isSucceeded();
        log.info("Clear scroll response:{}", succeeded);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (iterator != null && iterator.hasNext()) {
            return false;
        } else {
            return searchScroll();
        }
    }

    private boolean searchScroll() throws IOException {
        SearchHit[] searchHits;
        if (scrollId == null) {
            SearchResponse searchResponse = rhlClient.search(searchRequest);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        } else {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            SearchResponse searchResponse = rhlClient.searchScroll(scrollRequest);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        List<Map<String, Object>> resultList = Lists.newArrayList();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> source = searchHit.getSourceAsMap();
            resultList.add(source);
        }

        iterator = resultList.iterator();
        return !iterator.hasNext();
    }

    public Elasticsearch6Config getElasticsearchConf() {
        return elasticsearchConfig;
    }

    public void setElasticsearchConf(Elasticsearch6Config elasticsearchConfig) {
        this.elasticsearchConfig = elasticsearchConfig;
    }
}
