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

package com.dtstack.flinkx.connector.dorisbatch.rest;

import com.dtstack.flinkx.connector.dorisbatch.options.DorisOptions;
import com.dtstack.flinkx.connector.dorisbatch.rest.module.RespContent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author tiezhu@dtstack.com
 * @since 08/10/2021 Friday
 */
public class DorisStreamLoad implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);

    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));
    private static final String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
    private String loadUrlStr;
    private String hostPort;
    private final String db;
    private final String tbl;
    private final String authEncoding;
    private final Properties streamLoadProp;

    public DorisStreamLoad(String hostPort, DorisOptions options) {
        this.hostPort = hostPort;
        this.db = options.getDatabase();
        this.tbl = options.getTable();
        this.loadUrlStr = String.format(loadUrlPattern, hostPort, db, tbl);
        this.authEncoding =
                Base64.getEncoder()
                        .encodeToString(
                                String.format("%s:%s", options.getUsername(), options.getPassword())
                                        .getBytes(StandardCharsets.UTF_8));
        this.streamLoadProp = options.getLoadProperties();
    }

    public String getLoadUrlStr() {
        return loadUrlStr;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
        this.loadUrlStr = String.format(loadUrlPattern, hostPort, this.db, this.tbl);
    }

    private HttpURLConnection getConnection(String urlStr, String label) throws IOException {
        // TODO use merge 来实现update功能

        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        for (Map.Entry<Object, Object> entry : streamLoadProp.entrySet()) {
            conn.addRequestProperty(
                    String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

        public LoadResponse(int status, String respMsg, String respContent) {
            this.status = status;
            this.respMsg = respMsg;
            this.respContent = respContent;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("status", status)
                    .append("respMsg", respMsg)
                    .append("respContent", respContent)
                    .toString();
        }
    }

    /**
     * Doris load data via stream.
     *
     * @param columnNames column name.
     * @param value the data load to doris.
     * @throws IOException io exception.
     */
    public void load(List<String> columnNames, String value) throws IOException {
        LoadResponse loadResponse = loadBatch(columnNames, value);
        LOG.debug("StreamLoad Response:{}", loadResponse);
        if (loadResponse.status != 200) {
            throw new ConnectException("stream load error, detail : " + loadResponse);
        } else {
            ObjectMapper obj = new ObjectMapper();
            RespContent respContent = obj.readValue(loadResponse.respContent, RespContent.class);
            if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                throw new IOException("stream load error: " + getDetailErrorLog(respContent));
            }
        }
    }

    private LoadResponse loadBatch(List<String> columnNames, String value) {
        String label = streamLoadProp.getProperty("label");
        if (StringUtils.isBlank(label)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String formatDate = sdf.format(new Date());
            label =
                    String.format(
                            "flink_connector_%s_%s",
                            formatDate, UUID.randomUUID().toString().replaceAll("-", ""));
        }

        HttpURLConnection beConn = null;
        try {
            // build request and send to new be location
            beConn = getConnection(loadUrlStr, label);
            // send data to be
            BufferedOutputStream bos = new BufferedOutputStream(beConn.getOutputStream());
            bos.write(value.getBytes());
            bos.close();

            // get respond
            int status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            InputStream stream = (InputStream) beConn.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            return new LoadResponse(status, respMsg, response.toString());

        } catch (Exception e) {
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
            LOG.warn(err, e);
            return new LoadResponse(-1, e.getMessage(), err);
        } finally {
            if (beConn != null) {
                beConn.disconnect();
            }
        }
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
                InputStream content = entity.getContent();
                BufferedReader br = new BufferedReader(new InputStreamReader(content));
                StringBuilder contentBuilder = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    contentBuilder.append(line);
                }
                return contentBuilder.toString();
            } else {
                return respContent.toString();
            }
        } catch (IOException e) {
            LOG.warn("Get detail error message failed. Error Url: " + respContent.getErrorURL());
            return respContent.getMessage();
        }
    }
}
