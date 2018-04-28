/**
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
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.IOException;
import java.util.List;

/**
 * The OutputFormat class of ElasticSearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsOutputFormat extends RichOutputFormat {

    protected String address;

    protected List<Integer> indexColumnIndices;

    protected List<String> indexColumnValues;

    protected List<String> indexColumnTypes;

    protected String type;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    private RestHighLevelClient client;

    private BulkRequest bulkRequest;


    @Override
    public void configure(Configuration configuration) {
        client = EsUtil.getClient(address);
        bulkRequest = new BulkRequest();
    }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        IndexRequest request = new IndexRequest(getIndex(row), type);
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
            String index = getIndex(row);
            IndexRequest request = new IndexRequest(index, type);
            request = request.source(EsUtil.rowToJsonMap(row, columnNames, columnTypes));
            bulkRequest.add(request);
            client.bulk(bulkRequest);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        if(client != null) {
            client.close();
        }
    }

    private String getIndex(Row record) throws WriteRecordException {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        try {
            for(; i < indexColumnIndices.size(); ++i) {
                Integer index = indexColumnIndices.get(i);
                String type =  indexColumnTypes.get(i);
                if(index == -1) {
                    String value = indexColumnValues.get(i);
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
