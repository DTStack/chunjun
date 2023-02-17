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
package com.dtstack.chunjun.connector.http.common;

import com.dtstack.chunjun.util.ExceptionUtil;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

@Slf4j
public class HttpUtil {
    private static final int COUNT = 32;
    private static final int TOTAL_COUNT = 1000;
    private static final int TIME_OUT = 5000;
    private static final int EXECUTION_COUNT = 5;

    public static Gson gson = new Gson();

    public static CloseableHttpClient getHttpClient() {

        return getBaseBuilder(TIME_OUT).build();
    }

    public static CloseableHttpClient getHttpsClient(int timeOut) {

        // 设置Http连接池
        SSLContext sslContext;
        try {
            sslContext =
                    new SSLContextBuilder()
                            .loadTrustMaterial(null, (certificate, authType) -> true)
                            .build();
        } catch (Exception e) {
            log.warn(ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        }
        return getBaseBuilder(timeOut)
                .setSSLContext(sslContext)
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .build();
    }

    public static HttpClientBuilder getBaseBuilder(int timeOut) {
        if (timeOut <= 0) {
            timeOut = TIME_OUT;
        }
        // 设置自定义的重试策略
        ServiceUnavailableRetryStrategyImpl strategy =
                new ServiceUnavailableRetryStrategyImpl.Builder()
                        .executionCount(EXECUTION_COUNT)
                        .retryInterval(1000)
                        .build();
        // 设置自定义的重试Handler
        HttpRequestRetryHandlerImpl retryHandler =
                new HttpRequestRetryHandlerImpl.Builder().executionCount(EXECUTION_COUNT).build();
        // 设置超时时间
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(timeOut)
                        .setConnectionRequestTimeout(timeOut)
                        .setSocketTimeout(timeOut)
                        .build();
        // 设置Http连接池
        PoolingHttpClientConnectionManager pcm = new PoolingHttpClientConnectionManager();
        pcm.setDefaultMaxPerRoute(COUNT);
        pcm.setMaxTotal(TOTAL_COUNT);

        return HttpClientBuilder.create()
                .setServiceUnavailableRetryStrategy(strategy)
                .setRetryHandler(retryHandler)
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(pcm);
    }

    public static HttpRequestBase getRequest(
            String method,
            Map<String, Object> requestBody,
            Map<String, String> header,
            String url) {
        log.debug("current request url: {}  current method:{} \n", url, method);
        HttpRequestBase request;

        if (HttpMethod.GET.name().equalsIgnoreCase(method)) {
            request = new HttpGet(url);
        } else if (HttpMethod.POST.name().equalsIgnoreCase(method)) {
            HttpPost post = new HttpPost(url);
            post.setEntity(getEntityData(requestBody));
            request = post;
        } else {
            throw new UnsupportedOperationException("Unsupported method:" + method);
        }

        if (MapUtils.isNotEmpty(header)) {
            for (Map.Entry<String, String> entry : header.entrySet()) {
                request.addHeader(entry.getKey(), entry.getValue());
            }
        }
        return request;
    }

    public static HttpRequestBase getRequest(
            String method,
            Map<String, Object> requestBody,
            Map<String, Object> requestParam,
            Map<String, Object> header,
            String url) {

        HttpRequestBase request;
        if (MapUtils.isNotEmpty(requestParam)) {
            ArrayList<String> params = new ArrayList<>();
            requestParam.forEach(
                    (k, v) -> {
                        try {
                            // 参数进行编码
                            params.add(
                                    URLEncoder.encode(k, StandardCharsets.UTF_8.name())
                                            + "="
                                            + URLEncoder.encode(
                                                    v instanceof Map
                                                            ? gson.toJson(v)
                                                            : v.toString(),
                                                    StandardCharsets.UTF_8.name()));
                        } catch (UnsupportedEncodingException e) {
                            throw new RuntimeException(
                                    "URLEncoder.encode k [" + k + "] or v [" + v + "] failed ", e);
                        }
                    });
            if (url.contains("?")) {
                url += "&" + String.join("&", params);
            } else {
                url += "?" + String.join("&", params);
            }
        }

        log.debug("current request url: {}  current method:{} \n", url, method);
        if (HttpMethod.GET.name().equalsIgnoreCase(method)) {
            request = new HttpGet(url);
        } else if (HttpMethod.POST.name().equalsIgnoreCase(method)) {
            HttpPost post = new HttpPost(url);
            post.setEntity(getEntityData(requestBody));
            request = post;
        } else {
            throw new UnsupportedOperationException("Unsupported method:" + method);
        }

        for (Map.Entry<String, Object> entry : header.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue().toString());
        }
        return request;
    }

    public static void closeClient(CloseableHttpClient httpClient) {
        try {
            httpClient.close();
        } catch (IOException e) {
            throw new RuntimeException("close client error");
        }
    }

    public static StringEntity getEntityData(Map<String, Object> body) {
        StringEntity stringEntity = new StringEntity(gson.toJson(body), StandardCharsets.UTF_8);
        stringEntity.setContentEncoding(StandardCharsets.UTF_8.name());
        return stringEntity;
    }
}
