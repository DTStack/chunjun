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
package com.dtstack.flinkx.restapi.common;

import com.google.gson.Gson;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/16
 */
public class HttpUtil {
    protected static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);
    private static final int COUNT = 32;
    private static final int TOTAL_COUNT = 1000;
    private static final int TIME_OUT = 5000;
    private static final int EXECUTION_COUNT = 5;

    public static Gson gson = new Gson();

    public static CloseableHttpClient getHttpClient() {
        // 设置自定义的重试策略
        MyServiceUnavailableRetryStrategy strategy = new MyServiceUnavailableRetryStrategy
                .Builder()
                .executionCount(EXECUTION_COUNT)
                .retryInterval(1000)
                .build();
        // 设置自定义的重试Handler
        MyHttpRequestRetryHandler retryHandler = new MyHttpRequestRetryHandler
                .Builder()
                .executionCount(EXECUTION_COUNT)
                .build();
        // 设置超时时间
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(TIME_OUT)
                .setConnectionRequestTimeout(TIME_OUT)
                .setSocketTimeout(TIME_OUT)
                .build();
        // 设置Http连接池
        PoolingHttpClientConnectionManager pcm = new PoolingHttpClientConnectionManager();
        pcm.setDefaultMaxPerRoute(COUNT);
        pcm.setMaxTotal(TOTAL_COUNT);

        return HttpClientBuilder.create()
                .setServiceUnavailableRetryStrategy(strategy)
                .setRetryHandler(retryHandler)
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(pcm)
                .build();
//        return HttpClientBuilder.create().build();
    }

    public static HttpRequestBase getRequest(String method,
                                             Map<String, Object> requestBody,
                                             Map<String, String> header,
                                             String url) {
        LOG.debug("current request url: {}  current method:{} \n", url, method);
        HttpRequestBase request = null;

        if (HttpMethod.GET.name().equalsIgnoreCase(method)) {
            request = new HttpGet(url);
        } else if (HttpMethod.POST.name().equalsIgnoreCase(method)) {
            HttpPost post = new HttpPost(url);
            post.setEntity(getEntityData(requestBody));
            request = post;
        } else {
            throw new UnsupportedOperationException("Unsupported method:" + method);
        }

        for (Map.Entry<String, String> entry : header.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
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
