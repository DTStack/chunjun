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

package com.dtstack.flinkx.es.writer;

import com.dtstack.flinkx.es.EsUtil;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The OutputFormat class of ElasticSearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsOutputFormat extends BaseRichOutputFormat {

    protected String address;

    protected String username;

    protected String password;

    protected List<Integer> idColumnIndices;

    protected List<String> idColumnValues;

    protected List<String> idColumnTypes;

    protected String index;

    protected String type;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    protected Map<String,Object> clientConfig;

    private transient RestHighLevelClient client;

    private transient BulkRequest bulkRequest;


    @Override
    public void configure(Configuration configuration) {
        client = EsUtil.getClient(address, username, password, clientConfig);
        bulkRequest = new BulkRequest();
    }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        String id = getId(row);
        IndexRequest request = StringUtils.isBlank(id) ? new IndexRequest(index, type) : new IndexRequest(index, type, id);
        request = request.source(EsUtil.rowToJsonMap(row, columnNames, columnTypes));
        try {
            client.index(request);
        } catch (Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        bulkRequest = new BulkRequest();
        for(Row row : rows) {
            String id = getId(row);
            IndexRequest request = StringUtils.isBlank(id) ? new IndexRequest(index, type) : new IndexRequest(index, type, id);
            request = request.source(EsUtil.rowToJsonMap(row, columnNames, columnTypes));
            bulkRequest.add(request);
        }

        BulkResponse response = client.bulk(bulkRequest);
        if (response.hasFailures()){
            processFailResponse(response);
        }
    }

    private void processFailResponse(BulkResponse response){
        BulkItemResponse[] itemResponses = response.getItems();
        WriteRecordException exception;
        for (int i = 0; i < itemResponses.length; i++) {
            if(itemResponses[i].isFailed()){
                if (dirtyDataManager != null){
                    exception = new WriteRecordException(itemResponses[i].getFailureMessage()
                            ,itemResponses[i].getFailure().getCause());
                    dirtyDataManager.writeData(rows.get(i), exception);
                }

                if (errCounter != null) {
                    errCounter.add(1);
                }
            }
        }
    }

    @Override
    public void closeInternal() throws IOException {
        if(client != null) {
            client.close();
        }
    }


    private String getId(Row record) throws WriteRecordException {
        if(idColumnIndices == null || idColumnIndices.size() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        int i = 0;
        try {
            for(; i < idColumnIndices.size(); ++i) {
                Integer index = idColumnIndices.get(i);
                String type =  idColumnTypes.get(i);
                if(index == -1) {
                    String value = idColumnValues.get(i);
                    sb.append(value);
                } else {
                    sb.append(StringUtil.col2string(record.getField(index), type));
                }
            }
        } catch(Exception ex) {
            String msg = getClass().getName() + " Writing record error: when converting field[" + i + "] in Row(" + record + ")";
            throw new WriteRecordException(msg, ex, i, record);
        }

        return sb.toString();
    }

}
