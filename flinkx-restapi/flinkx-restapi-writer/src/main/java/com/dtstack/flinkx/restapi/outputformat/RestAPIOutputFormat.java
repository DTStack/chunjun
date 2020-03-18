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
import com.dtstack.flinkx.restapi.common.HttpMethod;
import com.dtstack.flinkx.restapi.common.HttpUtil;
import com.dtstack.flinkx.restapi.common.MyHttpRequestRetryHandler;
import com.dtstack.flinkx.restapi.common.MyServiceUnavailableRetryStrategy;
import com.dtstack.flinkx.util.JsonUtils;
import org.apache.avro.data.Json;
import org.apache.flink.types.Row;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 * 当前只考虑了元数据读取，和带有字段名column读取的情况，其他情况暂未考虑
 */
public class RestAPIOutputFormat extends RichOutputFormat {

    protected String url;
    protected Map<String, Object> header;
    protected String method;
    protected Map<String, Object> body;
    protected ArrayList<String> column;
    protected Map<String, Object> params;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        // Nothing to do
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        CloseableHttpClient httpClient = HttpUtil.getHttpClient();
        int index = 0;
        Map<String, Object> columnData = new HashMap<>();
        String dataRow = "";
        try {
            if (column != null) {
                for (; index < row.getArity(); index++) {
                    columnData.put(column.get(index), row.getField(index));
                }
                dataRow = JsonUtils.objectToJsonStr(columnData);
            } else {
                dataRow = row.getField(index).toString();
            }

            body.put("data", JsonUtils.jsonStrToObject(dataRow, Map.class));
            Iterator iterator = params.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                body.put((String) entry.getKey(), entry.getValue());
            }

            HttpRequestBase request = HttpUtil.getRequest(method, body, url);

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
        List<Object> dataRow = new ArrayList<>();
        Map<String, Object> temp = new HashMap<>();
        int index = 0;
        // rows 用于批量写入数据
        for (Row row : rows) {
            if (!column.isEmpty()) {
                for (; index < column.size(); index++) {
                    temp.put(column.get(index), row.getField(index));
                    dataRow.add(temp);
                }
            } else {
                dataRow.add(row.getField(index));
            }
        }
        body.put("data", JsonUtils.objectToJsonStr(dataRow));
        Iterator iterator = params.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            body.put((String) entry.getKey(), entry.getValue());
        }
    }

    private void requestErrorMessage(Exception e, int index, Row row) {
        if (e instanceof IOException) {
            throw new RuntimeException(e.getMessage());
        }

        if (index < row.getArity()) {
            recordConvertDetailErrorMessage(index, row);
            throw new RuntimeException("添加脏数据:" + row.getField(index));
        }
        throw new RuntimeException("request error");
    }

}
