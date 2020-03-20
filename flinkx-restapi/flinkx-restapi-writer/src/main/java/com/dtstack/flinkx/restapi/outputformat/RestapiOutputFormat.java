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
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.restapi.common.HttpUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.*;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 * 当前只考虑了元数据读取，和带有字段名column读取的情况，其他情况暂未考虑
 */
public class RestapiOutputFormat extends RichOutputFormat {

    protected String url;
    protected String method;
    protected ArrayList<String> column;
    protected Map<String, Object> params;
    protected Map<String, Object> body;
    protected Map<String, String> header;

    private transient static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        // Nothing to do
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        LOG.info("start write single record");
        CloseableHttpClient httpClient = HttpUtil.getHttpClient();
        int index = 0;
        Map<String, Object> columnData = new HashMap<>();
        Map<String, Object> requestBody = new HashMap<>();
        List<Object> dataRow = new ArrayList<>();
        try {
            if (!column.isEmpty()) {
                for (; index < row.getArity(); index++) {
                    columnData.put(column.get(index), row.getField(index));
                }
                dataRow.add(objectMapper.writeValueAsString(columnData));
            } else {
                // 以下只针对元数据同步采集情况
                dataRow.add(objectMapper.readValue(row.getField(index).toString(), Map.class).get("data"));
            }
//            dataRow = getDataFromRow(row, column);
            if (!params.isEmpty()) {
                Iterator iterator = params.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    body.put((String) entry.getKey(), entry.getValue());
                }
            }
            body.put("data", dataRow);
            requestBody.put("json", body);
            HttpRequestBase request = HttpUtil.getRequest(method, requestBody, header, url);

            System.out.println(objectMapper.writeValueAsString(requestBody));

            CloseableHttpResponse httpResponse = httpClient.execute(request);
            // 重试之后返回状态码不为200
            if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                LOG.warn("当前请求状态码为" + httpResponse.getStatusLine().getStatusCode());
                throw new IOException("经过重试之后请求响应仍不成功");
            }
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
            Map<String, Object> temp = new HashMap<>();
            Map<String, Object> requestBody = new HashMap<>();
            int index = 0;
            // rows 用于批量写入数据
            for (Row row : rows) {
                if (!column.isEmpty()) {
                    for (; index < column.size(); index++) {
                        temp.put(column.get(index), row.getField(index));
                        dataRow.add(temp);
                    }
                } else {
                    dataRow.add((objectMapper.readValue(row.getField(index).toString(), Map.class)).get("data"));
                }
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
            System.out.println(objectMapper.writeValueAsString(requestBody));
            HttpRequestBase request = HttpUtil.getRequest(method, requestBody, header, url);
            CloseableHttpResponse httpResponse = httpClient.execute(request);
            // 重试之后返回状态码不为200
            if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                LOG.warn("当前请求状态码为" + httpResponse.getStatusLine().getStatusCode());
                throw new IOException("经过重试之后请求响应仍不成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void requestErrorMessage(Exception e, int index, Row row) {
        if (e instanceof IOException) {

        }

        if (index < row.getArity()) {
            recordConvertDetailErrorMessage(index, row);
            LOG.warn("添加脏数据:" + row.getField(index));
        }
    }

    private List<Object> getDataFromRow(Row row, List<String> column) throws IOException {
        List<Object> result = new ArrayList<>();
        Map<String, Object> columnData = new HashMap<>();
        int index = 0;
        if (!column.isEmpty()) {
            // 如果column不为空，那么将数据和字段名一一对应
            for (; index < row.getArity(); index++) {
                columnData.put(column.get(index), row.getField(index));
            }
            result.add(objectMapper.writeValueAsString(columnData));
        } else {
            // 以下只针对元数据同步采集情况
            result.add(objectMapper.readValue(row.getField(index).toString(), Map.class).get("data"));
        }
        return result;
    }
}
