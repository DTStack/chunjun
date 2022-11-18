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

package com.dtstack.chunjun.connector.elasticsearch6.sink;

import com.dtstack.chunjun.connector.elasticsearch.KeyExtractor;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6ClientFactory;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6Config;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6RequestFactory;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Elasticsearch6OutputFormat extends BaseRichOutputFormat {

    /** Elasticsearch Configuration */
    private Elasticsearch6Config elasticsearchConf;

    /** Elasticsearch High Level Client */
    private transient RestHighLevelClient rhlClient;

    private transient BulkRequest bulkRequest;

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            DocWriteRequest docWriteRequest;
            switch (rowData.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    docWriteRequest = processUpsert(rowData);
                    if (docWriteRequest instanceof IndexRequest) {
                        rhlClient.index((IndexRequest) docWriteRequest);
                    } else {
                        rhlClient.update((UpdateRequest) docWriteRequest);
                    }
                    break;
                case DELETE:
                case UPDATE_BEFORE:
                    docWriteRequest = processDelete(rowData);
                    rhlClient.delete((DeleteRequest) docWriteRequest);
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
        bulkRequest = new BulkRequest();
        DocWriteRequest docWriteRequest;
        for (RowData rowData : rows) {
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
        BulkResponse response = rhlClient.bulk(bulkRequest);
        if (response.hasFailures()) {
            processFailResponse(response);
        }
    }

    private void processFailResponse(BulkResponse response) {
        BulkItemResponse[] itemResponses = response.getItems();
        WriteRecordException exception;
        for (int i = 0; i < itemResponses.length; i++) {
            if (itemResponses[i].isFailed()) {
                if (dirtyDataManager != null) {
                    exception =
                            new WriteRecordException(
                                    itemResponses[i].getFailureMessage(),
                                    itemResponses[i].getFailure().getCause());
                    dirtyDataManager.writeData(rows.get(i), exception);
                }

                if (errCounter != null) {
                    errCounter.add(1);
                }
            }
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        rhlClient = Elasticsearch6ClientFactory.createClient(elasticsearchConf);
    }

    @Override
    protected void closeInternal() throws IOException {
        if (rhlClient != null) {
            rhlClient.close();
        }
    }

    public Elasticsearch6Config getElasticsearchConf() {
        return elasticsearchConf;
    }

    public void setElasticsearchConf(Elasticsearch6Config elasticsearchConf) {
        this.elasticsearchConf = elasticsearchConf;
    }

    private DocWriteRequest processUpsert(RowData rowData) throws Exception {
        Map<String, Object> message =
                (Map<String, Object>)
                        rowConverter.toExternal(rowData, new HashMap<String, Object>());

        if (elasticsearchConf.getIds() == null || elasticsearchConf.getIds().size() == 0) {
            IndexRequest indexRequest =
                    Elasticsearch6RequestFactory.createIndexRequest(
                            elasticsearchConf.getIndex(), elasticsearchConf.getType(), message);
            return indexRequest;
        } else {
            final String key =
                    KeyExtractor.getDocId(
                            elasticsearchConf.getIds(),
                            message,
                            elasticsearchConf.getKeyDelimiter());
            UpdateRequest updateRequest =
                    Elasticsearch6RequestFactory.createUpdateRequest(
                            elasticsearchConf.getIndex(),
                            elasticsearchConf.getType(),
                            key,
                            message);
            return updateRequest;
        }
    }

    private DeleteRequest processDelete(RowData rowData) throws Exception {
        Map<String, Object> message =
                (Map<String, Object>)
                        rowConverter.toExternal(rowData, new HashMap<String, Object>());

        final String key =
                KeyExtractor.getDocId(
                        elasticsearchConf.getIds(), message, elasticsearchConf.getKeyDelimiter());
        DeleteRequest deleteRequest =
                Elasticsearch6RequestFactory.createDeleteRequest(
                        elasticsearchConf.getIndex(), elasticsearchConf.getType(), key);
        return deleteRequest;
    }
}
