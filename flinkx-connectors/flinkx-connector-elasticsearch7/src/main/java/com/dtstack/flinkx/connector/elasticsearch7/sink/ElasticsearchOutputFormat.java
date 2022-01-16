package com.dtstack.flinkx.connector.elasticsearch7.sink;

import com.dtstack.flinkx.connector.elasticsearch.KeyExtractor;
import com.dtstack.flinkx.connector.elasticsearch.table.IndexGenerator;
import com.dtstack.flinkx.connector.elasticsearch7.Elasticsearch7ClientFactory;
import com.dtstack.flinkx.connector.elasticsearch7.Elasticsearch7RequestFactory;
import com.dtstack.flinkx.connector.elasticsearch7.ElasticsearchConf;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 17:19
 */
public class ElasticsearchOutputFormat extends BaseRichOutputFormat {

    /** Elasticsearch Configuration */
    ElasticsearchConf elasticsearchConf;

    /** Elasticsearch High Level Client */
    private transient RestHighLevelClient rhlClient;

    private transient BulkRequest bulkRequest;

    private final IndexGenerator indexGenerator;

    public ElasticsearchOutputFormat(
            ElasticsearchConf elasticsearchConf, IndexGenerator indexGenerator) {
        this.elasticsearchConf = elasticsearchConf;
        this.indexGenerator = indexGenerator;
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
        bulkRequest = new BulkRequest();
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
        rhlClient =
                Elasticsearch7ClientFactory.createClient(
                        elasticsearchConf, getRuntimeContext().getDistributedCache());
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

        if (elasticsearchConf.getIds() == null || elasticsearchConf.getIds().size() == 0) {
            IndexRequest indexRequest =
                    Elasticsearch7RequestFactory.createIndexRequest(
                            indexGenerator.generate(rowData), message);
            return indexRequest;
        } else {
            final String key =
                    KeyExtractor.getDocId(
                            elasticsearchConf.getIds(),
                            message,
                            elasticsearchConf.getKeyDelimiter());
            UpdateRequest updateRequest =
                    Elasticsearch7RequestFactory.createUpdateRequest(
                            indexGenerator.generate(rowData), key, message);
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
                Elasticsearch7RequestFactory.createDeleteRequest(elasticsearchConf.getIndex(), key);
        return deleteRequest;
    }
}
