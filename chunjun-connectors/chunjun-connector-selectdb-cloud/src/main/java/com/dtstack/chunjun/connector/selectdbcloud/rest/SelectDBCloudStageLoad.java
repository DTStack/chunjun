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

package com.dtstack.chunjun.connector.selectdbcloud.rest;

import com.dtstack.chunjun.connector.selectdbcloud.exception.SelectdbcloudRuntimeException;
import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConfig;
import com.dtstack.chunjun.connector.selectdbcloud.rest.http.HttpPostBuilder;
import com.dtstack.chunjun.connector.selectdbcloud.rest.http.HttpPutBuilder;
import com.dtstack.chunjun.connector.selectdbcloud.rest.model.BaseResponse;
import com.dtstack.chunjun.connector.selectdbcloud.rest.model.CopyIntoResp;
import com.dtstack.chunjun.connector.selectdbcloud.sink.CopySQLBuilder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class SelectDBCloudStageLoad implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SelectDBCloudStageLoad.class);
    public static final int SUCCESS = 0;
    public static final String FAIL = "1";
    private static final Pattern COMMITTED_PATTERN =
            Pattern.compile(
                    "errCode = 2, detailMessage = No files can be copied, matched (\\d+) files, "
                            + "filtered (\\d+) files because files may be loading or loaded");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";
    private static final String COPY_URL_PATTERN = "http://%s/copy/query";

    private String user;
    private String passwd;
    private String loadUrlStr;
    private String hostPort;
    private final HttpClientBuilder httpClientBuilder =
            HttpClients.custom().disableRedirectHandling();
    private CloseableHttpClient httpClient;
    private SelectdbcloudConfig conf;

    public SelectDBCloudStageLoad(SelectdbcloudConfig conf) {
        this.hostPort = conf.getHttpUrl();
        this.user = conf.getUsername();
        this.passwd = conf.getPassword();
        this.loadUrlStr = String.format(UPLOAD_URL_PATTERN, hostPort);
        this.httpClient = httpClientBuilder.build();
        this.conf = conf;
    }

    public String getLoadUrlStr() {
        return loadUrlStr;
    }

    public void load(String value) throws IOException {
        String fileName = UUID.randomUUID().toString();
        String address = getUploadAddress(fileName);
        upLoadFile(address, value, fileName);
        executeCopy(fileName);
    }

    /** execute copy into */
    public void executeCopy(String fileName) throws IOException {
        long start = System.currentTimeMillis();
        CopySQLBuilder copySQLBuilder = new CopySQLBuilder(conf, fileName);
        String copySQL = copySQLBuilder.buildCopySQL();
        LOG.info("build copy SQL is {}", copySQL);
        Map<String, String> params = new HashMap<>();
        params.put("sql", copySQL);
        HttpPostBuilder postBuilder = new HttpPostBuilder();
        postBuilder
                .setUrl(String.format(COPY_URL_PATTERN, hostPort))
                .baseAuth(conf.getUsername(), conf.getPassword())
                .setEntity(new StringEntity(OBJECT_MAPPER.writeValueAsString(params)));

        try (CloseableHttpResponse response = httpClient.execute(postBuilder.build())) {
            final int statusCode = response.getStatusLine().getStatusCode();
            final String reasonPhrase = response.getStatusLine().getReasonPhrase();
            String loadResult = "";
            if (statusCode != 200) {
                LOG.warn(
                        "commit failed with status {} {}, reason {}",
                        statusCode,
                        hostPort,
                        reasonPhrase);
                throw new SelectdbcloudRuntimeException("commit error with file: " + fileName);
            } else if (response.getEntity() != null) {
                loadResult = EntityUtils.toString(response.getEntity());
                boolean success = handleCommitResponse(loadResult);
                if (success) {
                    LOG.info(
                            "commit success cost {}ms, response is {}",
                            System.currentTimeMillis() - start,
                            loadResult);
                } else {
                    throw new SelectdbcloudRuntimeException("commit failed with file: " + fileName);
                }
            }
        }
    }

    public boolean handleCommitResponse(String loadResult) throws IOException {
        BaseResponse<CopyIntoResp> baseResponse =
                OBJECT_MAPPER.readValue(
                        loadResult, new TypeReference<BaseResponse<CopyIntoResp>>() {});
        if (baseResponse.getCode() == SUCCESS) {
            CopyIntoResp dataResp = baseResponse.getData();
            if (FAIL.equals(dataResp.getDataCode())) {
                LOG.error("copy into execute failed, reason:{}", loadResult);
                return false;
            } else {
                Map<String, String> result = dataResp.getResult();
                if (!result.get("state").equals("FINISHED") && !isCommitted(result.get("msg"))) {
                    LOG.error("copy into load failed, reason:{}", loadResult);
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            LOG.error("commit failed, reason:{}", loadResult);
            return false;
        }
    }

    public static boolean isCommitted(String msg) {
        return COMMITTED_PATTERN.matcher(msg).matches();
    }

    /** Upload File */
    public void upLoadFile(String address, String value, String fileName) throws IOException {
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder
                .setUrl(address)
                .addCommonHeader()
                .setEntity(
                        new InputStreamEntity(
                                new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8))));
        try (CloseableHttpResponse response = httpClient.execute(putBuilder.build())) {
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                String result =
                        response.getEntity() == null
                                ? null
                                : EntityUtils.toString(response.getEntity());
                LOG.error("upload file {} error, response {}", fileName, result);
                throw new SelectdbcloudRuntimeException("upload file error: " + fileName);
            }
        }
    }

    /** Get the redirected s3 address */
    public String getUploadAddress(String fileName) throws IOException {
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder
                .setUrl(loadUrlStr)
                .addFileName(fileName)
                .addCommonHeader()
                .setEmptyEntity()
                .baseAuth(user, passwd);

        try (CloseableHttpResponse execute = httpClient.execute(putBuilder.build())) {
            int statusCode = execute.getStatusLine().getStatusCode();
            String reason = execute.getStatusLine().getReasonPhrase();
            if (statusCode == 307) {
                Header location = execute.getFirstHeader("location");
                String uploadAddress = location.getValue();
                LOG.info("redirect to s3:{}", uploadAddress);
                return uploadAddress;
            } else {
                HttpEntity entity = execute.getEntity();
                String result = entity == null ? null : EntityUtils.toString(entity);
                LOG.error(
                        "Failed get the redirected address, status {}, reason {}, response {}",
                        statusCode,
                        reason,
                        result);
                throw new RuntimeException("Could not get the redirected address.");
            }
        }
    }

    public void close() throws IOException {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.error("Closing httpClient failed.", e);
                throw new RuntimeException("Closing httpClient failed.", e);
            }
        }
    }
}
