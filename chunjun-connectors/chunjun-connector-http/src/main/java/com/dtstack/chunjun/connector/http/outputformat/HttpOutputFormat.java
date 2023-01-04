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
package com.dtstack.chunjun.connector.http.outputformat;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.http.common.HttpUtil;
import com.dtstack.chunjun.connector.http.common.HttpWriterConfig;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.ThreadUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.dtstack.chunjun.connector.http.common.HttpKeys.KEY_BATCH;

@Slf4j
public class HttpOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = -561611231821621445L;

    protected HttpWriterConfig httpWriterConfig;

    protected static final int DEFAULT_TIME_OUT = 300000;

    protected Gson gson;

    @Override
    protected void preCommit() {}

    @Override
    protected void closeInternal() {}

    @Override
    public void rollback(long checkpointId) {}

    @Override
    public void commit(long checkpointId) {}

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        httpWriterConfig.getParams().put("threadId", UUID.randomUUID().toString().substring(0, 8));
        gson = new GsonBuilder().serializeNulls().create();
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) throws WriteRecordException {
        if (httpWriterConfig.getIsAssert()) {
            rows.add(row);
            httpRequestForAssert();
            return;
        }
        httpRequest(row);
    }

    private void httpRequest(RowData row) throws WriteRecordException {
        log.info("start write single record");
        CloseableHttpClient httpClient = HttpUtil.getHttpClient();
        Map<String, Object> mapData = getMapDataFromRow(row, httpWriterConfig.getColumn());
        try {
            sendRequest(
                    httpClient,
                    mapData,
                    httpWriterConfig.getMethod(),
                    httpWriterConfig.getHeader(),
                    httpWriterConfig.getUrl());
            ThreadUtil.sleepMilliseconds(httpWriterConfig.getDelay());
        } catch (IOException e) {
            throw new ChunJunRuntimeException(e);
        } finally {
            HttpUtil.closeClient(httpClient);
        }
    }

    /** only for data assert */
    private void httpRequestForAssert() {
        try {
            CloseableHttpClient httpClient = HttpUtil.getHttpClient();
            List<Object> dataRow = new ArrayList<>();
            Map<String, Object> requestBody = Maps.newHashMap();
            for (RowData row : rows) {
                dataRow.add(getDataFromRow(row, httpWriterConfig.getColumns()));
            }
            httpWriterConfig
                    .getParams()
                    .put(KEY_BATCH, UUID.randomUUID().toString().substring(0, 8));
            for (Map.Entry<String, Object> stringObjectEntry :
                    httpWriterConfig.getParams().entrySet()) {
                httpWriterConfig
                        .getFormatBody()
                        .put(stringObjectEntry.getKey(), stringObjectEntry.getValue());
            }
            httpWriterConfig.getFormatBody().put("data", dataRow);
            requestBody.put("json", httpWriterConfig.getFormatBody());
            log.info("this batch size = {}, send data:{}", rows.size(), gson.toJson(requestBody));
            sendRequest(
                    httpClient,
                    requestBody,
                    httpWriterConfig.getMethod(),
                    httpWriterConfig.getFormatHeader(),
                    httpWriterConfig.getUrl());
        } catch (Exception e) {
            log.error(ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        }
    }

    // only for data assert batch commit
    @Override
    protected void writeMultipleRecordsInternal() {
        log.info("start write multiple records");
        if (httpWriterConfig.getIsAssert()) {
            httpRequestForAssert();
        }
    }

    private Object getDataFromRow(RowData row, List<String> column) {
        Map<String, Object> columnData = Maps.newHashMap();
        int index = 0;
        if (!column.isEmpty()) {
            // if column is not empty ,row one to one column
            for (; index < row.getArity(); index++) {
                columnData.put(column.get(index), ((ColumnRowData) row).getField(index).getData());
            }
            return gson.toJson(columnData);
        } else {
            return ((ColumnRowData) row).getField(index).getData();
        }
    }

    private Map<String, Object> getMapDataFromRow(RowData row, List<FieldConfig> columns)
            throws WriteRecordException {
        RowData rowData;
        try {
            rowData = (RowData) rowConverter.toExternal(row, new GenericRowData(row.getArity()));
        } catch (Exception e) {
            throw new WriteRecordException(e.getMessage(), e);
        }
        Map<String, Object> columnData = Maps.newHashMap();
        int index = 0;
        if (!columns.isEmpty()) {
            // if column is not empty ,row one to one column
            for (; index < columns.size(); index++) {
                FieldConfig fieldConfig = columns.get(index);
                if (row instanceof GenericRowData) {
                    columnData.put(
                            fieldConfig.getName(), ((GenericRowData) rowData).getField(index));
                } else {
                    columnData.put(
                            fieldConfig.getName(),
                            ((ColumnRowData) rowData).getField(index).getData());
                }
            }
        }
        return columnData;
    }

    private void sendRequest(
            CloseableHttpClient httpClient,
            Map<String, Object> requestBody,
            String method,
            Map<String, String> header,
            String url)
            throws IOException {
        log.debug("send data:{}", gson.toJson(requestBody));
        HttpRequestBase request = HttpUtil.getRequest(method, requestBody, header, url);
        // set request timeout
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(DEFAULT_TIME_OUT)
                        .setConnectionRequestTimeout(DEFAULT_TIME_OUT)
                        .setSocketTimeout(DEFAULT_TIME_OUT)
                        .build();
        request.setConfig(requestConfig);
        CloseableHttpResponse httpResponse = httpClient.execute(request);
        if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            log.warn("request retry code is " + httpResponse.getStatusLine().getStatusCode());
        }
    }
}
