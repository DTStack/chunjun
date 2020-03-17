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

import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/16
 */
public class HttpUtil {
    private static final int COUNT = 32;
    private static final int TOTAL_COUNT = 1000;

    public static CloseableHttpClient getHttpClient() {
        // 设置自定义的重试策略
        MyServiceUnavailableRetryStrategy strategy = new MyServiceUnavailableRetryStrategy
                .Builder()
                .executionCount(5)
                .retryInterval(1000)
                .build();
        // 设置自定义的重试Handler
        MyHttpRequestRetryHandler retryHandler = new MyHttpRequestRetryHandler
                .Builder()
                .executionCount(5)
                .build();
        // 设置超时时间
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setConnectionRequestTimeout(5000)
                .setSocketTimeout(5000)
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
    }

    public static HttpRequestBase getRequest(String method, Map<String, Object> requestBody, String url) {
        HttpRequestBase request = null;
        if (HttpMethod.GET.name().equalsIgnoreCase(method)) {
            request = new HttpGet(url);
        }

        if (HttpMethod.POST.name().equalsIgnoreCase(method)) {
            HttpPost post = new HttpPost(url);
            post.setEntity(getEntityData(requestBody));
            request = post;
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

    public static UrlEncodedFormEntity getEntityData(Map<String, Object> body) {
        List<NameValuePair> urlParameters = new ArrayList<>();
        for (Map.Entry<String, Object> entry : body.entrySet()) {
            urlParameters.add(new BasicNameValuePair(
                    entry.getKey(), entry.getValue().toString()));
        }
        try {
            return new UrlEncodedFormEntity(urlParameters);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("set entity error");
        }
    }
}
