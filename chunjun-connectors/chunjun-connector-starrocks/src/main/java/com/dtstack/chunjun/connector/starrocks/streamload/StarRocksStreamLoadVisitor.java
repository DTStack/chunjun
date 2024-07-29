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

package com.dtstack.chunjun.connector.starrocks.streamload;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.dtstack.chunjun.connector.starrocks.util.StarRocksUtil.getBasicAuthHeader;

@Slf4j
public class StarRocksStreamLoadVisitor implements Serializable {
    private static final long serialVersionUID = 2159752421788141274L;

    private static final int ERROR_log_MAX_LENGTH = 3000;

    private final StarRocksConfig starRocksConfig;
    private long pos;
    private static final String RESULT_FAILED = "Fail";
    private static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    private static final String LAEBL_STATE_VISIBLE = "VISIBLE";
    private static final String LAEBL_STATE_COMMITTED = "COMMITTED";
    private static final String RESULT_LABEL_PREPARE = "PREPARE";
    private static final String RESULT_LABEL_ABORTED = "ABORTED";
    private static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";

    public StarRocksStreamLoadVisitor(StarRocksConfig starRocksConfig) {
        this.starRocksConfig = starRocksConfig;
    }

    public void doStreamLoad(StarRocksSinkBufferEntity bufferEntity) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the hosts in `load_url` could be connected.");
        }
        String loadUrl =
                host
                        + "/api/"
                        + bufferEntity.getDatabase()
                        + "/"
                        + bufferEntity.getTable()
                        + "/_stream_load";
        log.info(String.format("Start to join batch data: label[%s].", bufferEntity.getLabel()));
        Map<String, Object> loadResult =
                doHttpPut(
                        loadUrl,
                        bufferEntity.getLabel(),
                        joinRows(bufferEntity.getBuffer(), (int) bufferEntity.getBatchSize()),
                        bufferEntity.getHttpHeadColumns());
        dealStreamLoadResult(host, bufferEntity, loadResult);
    }

    private void dealStreamLoadResult(
            String host, StarRocksSinkBufferEntity bufferEntity, Map<String, Object> loadResult)
            throws IOException {
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException(
                    "Unable to flush data to StarRocks: unknown result status, usually caused by: 1.authorization or permission related problems. 2.Wrong column_separator or row_delimiter. 3.Column count exceeded the limitation.");
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
        }
        if (RESULT_FAILED.equals(loadResult.get(keyStatus))) {
            Map<String, String> logMap = new HashMap<>();
            if (loadResult.containsKey("ErrorURL")) {
                logMap.put("streamLoadErrorLog", getErrorLog((String) loadResult.get("ErrorURL")));
            }
            throw new StarRocksStreamLoadFailedException(
                    String.format(
                            "Failed to flush data to StarRocks, Error " + "response: \n%s\n%s\n",
                            JSON.toJSONString(loadResult), JSON.toJSONString(logMap)),
                    loadResult,
                    bufferEntity);
        } else if (RESULT_LABEL_EXISTED.equals(loadResult.get(keyStatus))) {
            log.error(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
            // has to block-checking the state to get the final result
            checkLabelState(host, bufferEntity);
        }
    }

    @SuppressWarnings("unchecked")
    private void checkLabelState(String host, StarRocksSinkBufferEntity bufferEntity)
            throws IOException {
        int idx = 0;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            } catch (InterruptedException ex) {
                break;
            }
            try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
                HttpGet httpGet =
                        new HttpGet(
                                host
                                        + "/api/"
                                        + starRocksConfig.getDatabase()
                                        + "/get_load_state?label="
                                        + bufferEntity.getLabel());
                httpGet.setHeader(
                        "Authorization",
                        getBasicAuthHeader(
                                starRocksConfig.getUsername(), starRocksConfig.getPassword()));
                httpGet.setHeader("Connection", "close");

                try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                    HttpEntity respEntity = getHttpEntity(resp);
                    if (respEntity == null) {
                        throw new StarRocksStreamLoadFailedException(
                                String.format(
                                        "Failed to flush data to StarRocks, Error "
                                                + "could not get the final state of label[%s].\n",
                                        bufferEntity.getLabel()),
                                null,
                                bufferEntity);
                    }
                    Map<String, Object> result =
                            (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
                    String labelState = (String) result.get("state");
                    if (null == labelState) {
                        throw new StarRocksStreamLoadFailedException(
                                String.format(
                                        "Failed to flush data to StarRocks, Error "
                                                + "could not get the final state of label[%s]. response[%s]\n",
                                        bufferEntity.getLabel(), EntityUtils.toString(respEntity)),
                                null,
                                bufferEntity);
                    }
                    log.info(
                            String.format(
                                    "Checking label[%s] state[%s]\n",
                                    bufferEntity.getLabel(), labelState));
                    switch (labelState) {
                        case LAEBL_STATE_VISIBLE:
                        case LAEBL_STATE_COMMITTED:
                            return;
                        case RESULT_LABEL_PREPARE:
                            continue;
                        case RESULT_LABEL_ABORTED:
                            throw new StarRocksStreamLoadFailedException(
                                    String.format(
                                            "Failed to flush data to StarRocks, Error "
                                                    + "label[%s] state[%s]\n",
                                            bufferEntity.getLabel(), labelState),
                                    null,
                                    bufferEntity,
                                    true);
                        case RESULT_LABEL_UNKNOWN:
                        default:
                            throw new StarRocksStreamLoadFailedException(
                                    String.format(
                                            "Failed to flush data to StarRocks, Error "
                                                    + "label[%s] state[%s]\n",
                                            bufferEntity.getLabel(), labelState),
                                    null,
                                    bufferEntity);
                    }
                }
            }
        }
    }

    private String getErrorLog(String errorUrl) {
        if (errorUrl == null || !errorUrl.startsWith("http")) {
            return null;
        }
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(errorUrl);
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) {
                    return null;
                }
                String errorLog = EntityUtils.toString(respEntity);
                if (errorLog != null && errorLog.length() > ERROR_log_MAX_LENGTH) {
                    errorLog = errorLog.substring(0, ERROR_log_MAX_LENGTH);
                }
                return errorLog;
            }
        } catch (Exception e) {
            log.warn("Failed to get error log.", e);
            return "Failed to get error log: " + e.getMessage();
        }
    }

    private String getAvailableHost() {
        List<String> hostList = starRocksConfig.getFeNodes();
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = "http://" + hostList.get((int) (pos % hostList.size()));
            if (tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(starRocksConfig.getLoadConfig().getHttpCheckTimeoutMs());
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            log.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes) {
        if (totalBytes < 0) {
            throw new RuntimeException(
                    "The ByteBuffer limit has been exceeded, json assembly may fail");
        }
        ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : 1 - rows.size()));
        bos.put("[".getBytes(StandardCharsets.UTF_8));
        byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
        boolean isFirstElement = true;
        for (byte[] row : rows) {
            if (!isFirstElement) {
                bos.put(jsonDelimiter);
            }
            bos.put(row, 1, row.length - 2);
            isFirstElement = false;
        }
        bos.put("]".getBytes(StandardCharsets.UTF_8));
        return bos.array();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(
            String loadUrl, String label, byte[] data, String httpHeadColumns) throws IOException {
        log.info(
                String.format(
                        "Executing stream load to: '%s', size: '%s', thread: %d",
                        loadUrl, data.length, Thread.currentThread().getId()));
        final HttpClientBuilder httpClientBuilder =
                HttpClients.custom()
                        .setRedirectStrategy(
                                new DefaultRedirectStrategy() {
                                    @Override
                                    protected boolean isRedirectable(String method) {
                                        return true;
                                    }
                                });
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            HttpPut httpPut = new HttpPut(loadUrl);
            Map<String, String> props = starRocksConfig.getLoadConfig().getHeadProperties();
            for (Map.Entry<String, String> entry : props.entrySet()) {
                httpPut.setHeader(entry.getKey(), entry.getValue());
            }
            if (!props.containsKey("columns") && StringUtils.isNotBlank(httpHeadColumns)) {
                httpPut.setHeader("columns", httpHeadColumns);
            }
            if (!httpPut.containsHeader("timeout")) {
                httpPut.setHeader("timeout", "60");
            }
            httpPut.setHeader("Expect", "100-continue");
            httpPut.setHeader("ignore_json_size", "true");
            httpPut.setHeader("strip_outer_array", "true");
            httpPut.setHeader("format", "json");
            httpPut.setHeader("label", label);
            httpPut.setHeader(
                    "Authorization",
                    getBasicAuthHeader(
                            starRocksConfig.getUsername(), starRocksConfig.getPassword()));
            httpPut.setEntity(new ByteArrayEntity(data));
            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) return null;
                return (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }

    private HttpEntity getHttpEntity(CloseableHttpResponse resp) {
        int code = resp.getStatusLine().getStatusCode();
        if (200 != code) {
            log.warn("Request failed with code:{}", code);
            return null;
        }
        HttpEntity respEntity = resp.getEntity();
        if (null == respEntity) {
            log.warn("Request failed with empty response.");
            return null;
        }
        return respEntity;
    }
}
