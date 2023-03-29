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

package com.dtstack.chunjun.connector.doris.rest;

import com.dtstack.chunjun.connector.doris.options.DorisConfig;
import com.dtstack.chunjun.connector.doris.rest.module.RespContent;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class DorisStreamLoad implements Serializable {
    private static final long serialVersionUID = 4973667902656840946L;

    private static final ObjectMapper OM = new ObjectMapper();
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load?";
    private final String authEncoding;
    private final Properties streamLoadProp;
    private String hostPort;
    private DorisConfig options;

    public DorisStreamLoad(DorisConfig options) {
        this.options = options;
        this.authEncoding =
                Base64.getEncoder()
                        .encodeToString(
                                String.format("%s:%s", options.getUsername(), options.getPassword())
                                        .getBytes(StandardCharsets.UTF_8));
        this.streamLoadProp = options.getLoadProperties();
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public void setOptions(DorisConfig options) {
        this.options = options;
    }

    /**
     * Generate Http Put request.
     *
     * @param columnNames doris table column names.
     * @param urlStr doris put url.
     * @param label the label of doris stream load.
     * @param mergeConditions the merge conditions of doris stream load.
     * @return http put request of doris stream load.
     */
    private HttpPut generatePut(
            List<String> columnNames, String urlStr, String label, String mergeConditions) {

        HttpPut httpPut = new HttpPut(urlStr);
        httpPut.setHeader("Authorization", "Basic " + authEncoding);
        httpPut.setHeader("Expect", "100-continue");
        httpPut.setHeader("Content-Type", "text/plain; charset=UTF-8");
        httpPut.setHeader("label", label);
        httpPut.setHeader("format", "json");
        // if body is list type ,strip_outer_array should be true
        httpPut.setHeader("strip_outer_array", "true");
        List<String> columns =
                columnNames.stream()
                        .map(this::quoteColumn)
                        .collect(Collectors.toCollection(LinkedList::new));
        httpPut.setHeader("columns", StringUtils.join(columns, ","));
        if (StringUtils.isNotBlank(mergeConditions)) {
            httpPut.setHeader("merge_type", "MERGE");
            httpPut.setHeader("delete", mergeConditions);
        } else {
            httpPut.setHeader("merge_type", "APPEND");
        }
        // httpPut.setHeader("column_separator", fieldDelimiter);
        // if (!"\n".equals(lineDelimiter)) {
        //    httpPut.setHeader("line_delimiter", lineDelimiter);
        // }
        for (Map.Entry<Object, Object> entry : streamLoadProp.entrySet()) {
            httpPut.setHeader(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        return httpPut;
    }

    private String quoteColumn(String column) {
        return "`" + column + "`";
    }

    public static class LoadResponse {
        public int status;
        public String respContent;

        public LoadResponse(int status, String respContent) {
            this.status = status;
            this.respContent = respContent;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("status", status)
                    .append("respContent", respContent)
                    .toString();
        }
    }

    public void replaceBackend() throws IOException {
        String backend = getBackend();
        this.setHostPort(backend);
        log.info("replace backend node to {}", backend);
    }

    private String getBackend() throws IOException {
        try {
            // get be url from fe
            return FeRestService.randomBackend(options);
        } catch (IOException e) {
            log.error("get backends info fail");
            throw new IOException(e);
        }
    }

    /**
     * Doris load data via stream.
     *
     * @param carrier data carrier.
     * @throws IOException io exception.
     */
    public void load(Carrier carrier) throws IOException {
        List<String> columnNames = carrier.getColumns();
        String loadUrlStr =
                String.format(
                        LOAD_URL_PATTERN, hostPort, carrier.getDatabase(), carrier.getTable());
        String json = OM.writeValueAsString(carrier.getInsertContent());
        String mergeConditions = carrier.getDeleteContent();
        LoadResponse loadResponse = loadBatch(columnNames, json, mergeConditions, loadUrlStr);
        log.debug("StreamLoad Response:{}", loadResponse);
        if (loadResponse.status != 200) {
            throw new ConnectException("stream load error, detail : " + loadResponse);
        } else {
            RespContent respContent = OM.readValue(loadResponse.respContent, RespContent.class);
            if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                log.error("stream load error url: " + respContent.getErrorURL());
                throw new IOException("stream load error: " + getDetailErrorLog(respContent));
            }
        }
    }

    private LoadResponse loadBatch(
            List<String> columnNames, String value, String mergeConditions, String loadUrlStr) {
        String label = generateLabel();

        final ConnectionConfig connectionConfig =
                ConnectionConfig.custom().setCharset(Charset.defaultCharset()).build();
        try (CloseableHttpClient httpclient =
                HttpClientBuilder.create().setDefaultConnectionConfig(connectionConfig).build()) {
            // build request and send to new be location
            HttpPut httpPut = generatePut(columnNames, loadUrlStr, label, mergeConditions);
            httpPut.setEntity(new ByteArrayEntity(value.getBytes()));

            HttpResponse response = httpclient.execute(httpPut);
            int status = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            return new LoadResponse(status, entity != null ? EntityUtils.toString(entity) : "");
        } catch (Exception e) {
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
            log.warn(err, e);
            return new LoadResponse(-1, err);
        }
    }

    /**
     * Generate the label of Doris Stream Load
     *
     * @return doris label
     */
    private String generateLabel() {
        String label = streamLoadProp.getProperty("label");
        if (StringUtils.isBlank(label)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String formatDate = sdf.format(new Date());
            label =
                    String.format(
                            "chunjun_connector_%s_%s",
                            formatDate, UUID.randomUUID().toString().replaceAll("-", ""));
        }
        return label;
    }

    /**
     * Get the detailed error log of the current operation.
     *
     * @param respContent response content from current operation.
     * @return the detailed error log
     */
    public String getDetailErrorLog(RespContent respContent) {
        try (CloseableHttpClient httpclient = HttpClientBuilder.create().build()) {
            if (StringUtils.isNotBlank(respContent.getErrorURL())) {
                HttpGet httpget = new HttpGet(respContent.getErrorURL());
                HttpResponse response = httpclient.execute(httpget);
                HttpEntity entity = response.getEntity();
                return EntityUtils.toString(entity);
            } else {
                return respContent.toString();
            }
        } catch (IOException e) {
            log.warn("Get detail error message failed. Error Url: " + respContent.getErrorURL());
            return respContent.getMessage();
        }
    }
}
