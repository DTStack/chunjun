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
package com.dtstack.flinkx.restapi.outputformat;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.restapi.common.HttpUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.google.common.collect.Maps;
import org.apache.flink.types.Row;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 * 当前只考虑了元数据读取，和带有字段名column读取的情况，其他情况暂未考虑
 */
public class RestapiOutputFormat extends BaseRichOutputFormat {

    protected String url;

    protected String method;

    protected ArrayList<String> column;

    protected Map<String, Object> params;

    protected Map<String, Object> body;

    protected Map<String, String> header;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        // Nothing to do
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        LOG.info("start write single record");
        CloseableHttpClient httpClient = HttpUtil.getHttpClient();
        int index = 0;
        Map<String, Object> requestBody = Maps.newHashMap();
        Object dataRow;
        try {
            dataRow = getDataFromRow(row, column);
            if (!params.isEmpty()) {
                Iterator iterator = params.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    body.put((String) entry.getKey(), entry.getValue());
                }
            }
            body.put("data", dataRow);
            requestBody.put("json", body);
            LOG.debug("当前发送的数据为:{}", GsonUtil.GSON.toJson(requestBody));
            sendRequest(httpClient, requestBody, method, header, url);
        } catch (Exception e) {
            requestErrorMessage(e, index, row);
        } finally {
            // 最后不管发送是否成功，都要关闭client
            HttpUtil.closeClient(httpClient);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        LOG.info("start write multiple records");
        try {
            CloseableHttpClient httpClient = HttpUtil.getHttpClient();
            List<Object> dataRow = new ArrayList<>();
            Map<String, Object> requestBody = Maps.newHashMap();
            for (Row row : rows) {
                dataRow.add(getDataFromRow(row, column));
            }
            if (!params.isEmpty()) {
                Iterator iterator = params.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    body.put((String) entry.getKey(), entry.getValue());
                }
            }
            body.put("data", dataRow);
            requestBody.put("json", body);
            LOG.debug("当前发送的数据为:{}", GsonUtil.GSON.toJson(requestBody));
            sendRequest(httpClient, requestBody, method, header, url);
        } catch (Exception e) {
            LOG.warn("write record error !", e);
        }
    }

    private void requestErrorMessage(Exception e, int index, Row row) {
        if (index < row.getArity()) {
            recordConvertDetailErrorMessage(index, row);
            LOG.warn("添加脏数据:" + row.getField(index));
        }
    }

    private Object getDataFromRow(Row row, List<String> column) throws IOException {
        Map<String, Object> columnData = Maps.newHashMap();
        int index = 0;
        if (!column.isEmpty()) {
            // 如果column不为空，那么将数据和字段名一一对应
            for (; index < row.getArity(); index++) {
                columnData.put(column.get(index), row.getField(index));
            }
            return GsonUtil.GSON.toJson(columnData);
        } else {
            return row.getField(index);
        }
    }


    private void sendRequest(CloseableHttpClient httpClient,
                             Map<String, Object> requestBody,
                             String method,
                             Map<String, String> header,
                             String url) throws IOException {
        LOG.debug("当前发送的数据为:{}", GsonUtil.GSON.toJson(requestBody));
        HttpRequestBase request = HttpUtil.getRequest(method, requestBody, header, url);
        CloseableHttpResponse httpResponse = httpClient.execute(request);
        // 重试之后返回状态码不为200
        if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            LOG.warn("重试之后当前请求状态码为" + httpResponse.getStatusLine().getStatusCode());
        }
    }
}
