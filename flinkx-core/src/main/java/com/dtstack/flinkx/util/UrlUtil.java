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

package com.dtstack.flinkx.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author jiangbo
 * @date 2018/7/10 14:08
 */
public class UrlUtil {

    private static int MAX_RETRY_TIMES = 3;

    private static int SLEEP_TIME_MILLI_SECOND = 2000;

    private static int CONNECTION_TIMEOUT_MILLI_SECOND = 3000;

    private static Charset charset = StandardCharsets.UTF_8;

    public static InputStream open(String url, int retryTimes) throws Exception {
        return RetryUtil.executeWithRetry(
                () -> {
                    // 设置超时时间，防止阻塞
                    URLConnection urlConnection = new URL(url).openConnection();
                    urlConnection.setConnectTimeout(CONNECTION_TIMEOUT_MILLI_SECOND);
                    urlConnection.setReadTimeout(CONNECTION_TIMEOUT_MILLI_SECOND);
                    return urlConnection.getInputStream();
                },
                retryTimes,
                SLEEP_TIME_MILLI_SECOND,
                false);
    }

    public static InputStream open(String url) throws Exception {
        return RetryUtil.executeWithRetry(
                () -> {
                    URLConnection urlConnection = new URL(url).openConnection();
                    urlConnection.setConnectTimeout(CONNECTION_TIMEOUT_MILLI_SECOND);
                    urlConnection.setReadTimeout(CONNECTION_TIMEOUT_MILLI_SECOND);
                    return urlConnection.getInputStream();
                },
                MAX_RETRY_TIMES,
                SLEEP_TIME_MILLI_SECOND,
                false);
    }

    public static void get(String url, long sleepTime, int retryTimes, Callback callback) {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            int num = 0;
            while (true) {
                String response = UrlUtil.get(httpClient, url);
                callback.call(response);
                if (callback.isReturn() || num > retryTimes) {
                    return;
                }

                num++;
                Thread.sleep(sleepTime);
            }
        } catch (Exception e) {
            callback.processError(e);
        }
    }

    public static String get(CloseableHttpClient httpClient, String url) throws Exception {
        return RetryUtil.executeWithRetry(
                () -> {
                    String respBody = null;
                    HttpGet httpGet = new HttpGet(url);
                    // 设置超时时间，防止阻塞
                    RequestConfig requestConfig =
                            RequestConfig.custom()
                                    .setSocketTimeout(CONNECTION_TIMEOUT_MILLI_SECOND)
                                    .setConnectTimeout(CONNECTION_TIMEOUT_MILLI_SECOND)
                                    .build();
                    httpGet.setConfig(requestConfig);
                    CloseableHttpResponse response = httpClient.execute(httpGet);

                    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                        HttpEntity entity = response.getEntity();
                        respBody = EntityUtils.toString(entity, charset);
                    }

                    response.close();
                    return respBody;
                },
                MAX_RETRY_TIMES,
                SLEEP_TIME_MILLI_SECOND,
                false);
    }

    public static interface Callback {

        /**
         * 请求结果处理函数
         *
         * @param response 请求响应结果
         */
        void call(String response);

        /**
         * 判断是否结束请求循环
         *
         * @return true:结束，false:继续请求
         */
        boolean isReturn();

        /**
         * 对异常进行处理的函数
         *
         * @param e 待处理的异常
         */
        void processError(Exception e);
    }
}
