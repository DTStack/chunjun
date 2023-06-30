/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.elasticsearch7.sink;

import com.dtstack.chunjun.connector.elasticsearch.KeyExtractor;
import com.dtstack.chunjun.connector.elasticsearch.table.IndexGenerator;
import com.dtstack.chunjun.connector.elasticsearch7.Elasticsearch7ClientFactory;
import com.dtstack.chunjun.connector.elasticsearch7.Elasticsearch7RequestFactory;
import com.dtstack.chunjun.connector.elasticsearch7.ElasticsearchConfig;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.sink.WriteMode;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ElasticsearchOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = 4075917714665517802L;

    /** Elasticsearch Configuration */
    ElasticsearchConfig elasticsearchConfig;

    /** Elasticsearch High Level Client */
    private transient RestHighLevelClient rhlClient;

    private final IndexGenerator indexGenerator;

    public ElasticsearchOutputFormat(
            ElasticsearchConfig elasticsearchConfig, IndexGenerator indexGenerator) {
        this.elasticsearchConfig = elasticsearchConfig;
        this.indexGenerator = indexGenerator;
    }

    @Override
    public void initializeGlobal(int parallelism) {
        if (WriteMode.OVERWRITE.name().equalsIgnoreCase(elasticsearchConfig.getWriteMode())) {
            DeleteByQueryRequest request = new DeleteByQueryRequest(elasticsearchConfig.getIndex());
            // 批量更新内容的时候，可能会遇到文档版本冲突的情况，需要设置版本冲突的时候如何处理。
            // 版本冲突解决方案如下：
            // proceed - 忽略版本冲突，继续执行
            // abort - 遇到版本冲突，中断执行
            request.setConflicts("proceed");
            // 设置删除时的查询条件
            request.setQuery(new MatchAllQueryBuilder());
            try (RestHighLevelClient rhlClient =
                    Elasticsearch7ClientFactory.createClient(elasticsearchConfig, null)) {
                // 执行请求
                BulkByScrollResponse bulkResponse =
                        rhlClient.deleteByQuery(request, RequestOptions.DEFAULT);
                // 操作消耗时间
                TimeValue timeTaken = bulkResponse.getTook();
                // 成功删除文档数量
                long deletedDocs = bulkResponse.getDeleted();
                log.info(
                        "Number of documents successfully deleted : "
                                + deletedDocs
                                + ", time : "
                                + timeTaken.toString());
            } catch (IOException e) {
                throw new ChunJunRuntimeException(
                        "cannot empty data by index : " + elasticsearchConfig.getIndex(), e);
            }
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            DocWriteRequest docWriteRequest;
            switch (rowData.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    docWriteRequest = processUpsert(rowData);
                    if (docWriteRequest instanceof IndexRequest) {
                        rhlClient.index((IndexRequest) docWriteRequest, RequestOptions.DEFAULT);
                    } else {
                        rhlClient.update((UpdateRequest) docWriteRequest, RequestOptions.DEFAULT);
                    }
                    break;
                case DELETE:
                case UPDATE_BEFORE:
                    docWriteRequest = processDelete(rowData);
                    rhlClient.delete((DeleteRequest) docWriteRequest, RequestOptions.DEFAULT);
                    break;
                default:
                    throw new RuntimeException("Unsupported row kind.");
            }

        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (RowData rowData : rows) {
            DocWriteRequest docWriteRequest;
            switch (rowData.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    docWriteRequest = processUpsert(rowData);
                    bulkRequest.add(docWriteRequest);
                    break;
                case DELETE:
                case UPDATE_BEFORE:
                    docWriteRequest = processDelete(rowData);
                    bulkRequest.add(docWriteRequest);
                    break;
                default:
                    throw new RuntimeException("Unsupported row kind.");
            }
        }
        BulkResponse response = rhlClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (response.hasFailures()) {
            processFailResponse(response);
        }
    }

    private void processFailResponse(BulkResponse response) {
        BulkItemResponse[] itemResponses = response.getItems();
        WriteRecordException exception;
        for (BulkItemResponse itemResponds : itemResponses) {
            if (itemResponds.isFailed()) {
                if (dirtyManager != null) {
                    exception =
                            new WriteRecordException(
                                    itemResponds.getFailureMessage(),
                                    itemResponds.getFailure().getCause());
                    long globalErrors =
                            accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
                    dirtyManager.collect(response, exception, null, globalErrors);
                }

                if (errCounter != null) {
                    errCounter.add(1);
                }
            }
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        rhlClient =
                Elasticsearch7ClientFactory.createClient(
                        elasticsearchConfig, getRuntimeContext().getDistributedCache());
        indexGenerator.open();
    }

    @Override
    protected void closeInternal() throws IOException {
        if (rhlClient != null) {
            rhlClient.close();
        }
    }

    private DocWriteRequest processUpsert(RowData rowData) throws Exception {
        Map<String, Object> message =
                (Map<String, Object>)
                        rowConverter.toExternal(rowData, new HashMap<String, Object>());

        if (elasticsearchConfig.getIds() == null || elasticsearchConfig.getIds().size() == 0) {
            IndexRequest indexRequest =
                    Elasticsearch7RequestFactory.createIndexRequest(
                            indexGenerator.generate(rowData), message);
            return indexRequest;
        } else {
            final String key =
                    KeyExtractor.getDocId(
                            elasticsearchConfig.getIds(),
                            message,
                            elasticsearchConfig.getKeyDelimiter());
            return Elasticsearch7RequestFactory.createUpdateRequest(
                    indexGenerator.generate(rowData), key, message);
        }
    }

    private DeleteRequest processDelete(RowData rowData) throws Exception {
        Map<String, Object> message =
                (Map<String, Object>)
                        rowConverter.toExternal(rowData, new HashMap<String, Object>());

        final String key =
                KeyExtractor.getDocId(
                        elasticsearchConfig.getIds(),
                        message,
                        elasticsearchConfig.getKeyDelimiter());
        return Elasticsearch7RequestFactory.createDeleteRequest(
                elasticsearchConfig.getIndex(), key);
    }
}
