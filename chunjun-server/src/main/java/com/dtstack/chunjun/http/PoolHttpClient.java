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
package com.dtstack.chunjun.http;

import com.dtstack.chunjun.util.RetryUtil;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @author xuchao
 * @date 2023-06-28
 */
public class PoolHttpClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolHttpClient.class);

    private static int SocketTimeout = 5000; // 5秒

    private static int ConnectTimeout = 5000; // 5秒

    // 将最大连接数增加到100
    private static int maxTotal = 100;

    // 将每个路由基础的连接增加到20
    private static int maxPerRoute = 20;

    private static int SLEEP_TIME_MILLI_SECOND = 2000;

    private static int DEFAULT_RETRY_TIMES = 3;

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static CloseableHttpClient httpClient = getHttpClient();

    private static Charset charset = Charset.forName("UTF-8");

    private static CloseableHttpClient getHttpClient() {
        ConnectionSocketFactory plainsf = PlainConnectionSocketFactory.getSocketFactory();
        LayeredConnectionSocketFactory sslsf = SSLConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> registry =
                RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("http", plainsf)
                        .register("https", sslsf)
                        .build();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
        cm.setMaxTotal(maxTotal);
        cm.setDefaultMaxPerRoute(maxPerRoute);

        // 设置请求和传输超时时间
        RequestConfig requestConfig =
                RequestConfig.custom()
                        // setConnectionRequestTimeout：设置从connect Manager获取Connection
                        // 超时时间，单位毫秒。这个属性是新加的属性，因为目前版本是可以共享连接池的。
                        .setConnectionRequestTimeout(ConnectTimeout)
                        // setSocketTimeout：请求获取数据的超时时间，单位毫秒。 如果访问一个接口，多少时间内无法返回数据，就直接放弃此次调用。
                        .setSocketTimeout(SocketTimeout)
                        // setConnectTimeout：设置连接超时时间，单位毫秒。
                        .setConnectTimeout(ConnectTimeout)
                        .build();

        return HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(cm)
                .setRetryHandler(new CJHttpRequestRetryHandler())
                .build();
    }

    public static String post(String url, Map<String, Object> bodyData) {
        return post(url, bodyData, null);
    }

    public static String post(String url, Object bodyData) {
        String responseBody = null;
        CloseableHttpResponse response = null;
        try {
            HttpPost httpPost = new HttpPost(url);

            httpPost.setHeader("Content-type", "application/json;charset=UTF-8");
            if (bodyData != null) {
                httpPost.setEntity(
                        new StringEntity(objectMapper.writeValueAsString(bodyData), charset));
            }

            // 请求数据
            response = httpClient.execute(httpPost);
            int status = response.getStatusLine().getStatusCode();
            if (status == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                // FIXME 暂时不从header读取
                responseBody = EntityUtils.toString(entity, charset);
            } else {
                LOGGER.warn(
                        "request url:{} fail:{}", url, response.getStatusLine().getStatusCode());
            }
        } catch (Exception e) {
            LOGGER.error("url:{}--->http request error:", url, e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
        }
        return responseBody;
    }

    public static String post(
            String url, Map<String, Object> bodyData, Map<String, Object> cookies) {
        return post(url, bodyData, cookies, Boolean.FALSE);
    }

    public static String post(
            String url,
            Map<String, Object> bodyData,
            Map<String, Object> cookies,
            Boolean isRedirect) {
        String responseBody = null;
        CloseableHttpResponse response = null;
        int status = 0;
        HttpPost httpPost = null;
        try {
            httpPost = new HttpPost(url);
            if (cookies != null && cookies.size() > 0) {
                httpPost.addHeader("Cookie", getCookieFormat(cookies));
            }

            httpPost.setHeader("Content-type", "application/json;charset=UTF-8");
            if (bodyData != null && bodyData.size() > 0) {
                httpPost.setEntity(
                        new StringEntity(objectMapper.writeValueAsString(bodyData), charset));
            }

            response = httpClient.execute(httpPost);
            // 请求数据
            status = response.getStatusLine().getStatusCode();
            if (status == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                // FIXME 暂时不从header读取
                responseBody = EntityUtils.toString(entity, charset);
            } else {
                LOGGER.warn(
                        "request url:{} fail:{}", url, response.getStatusLine().getStatusCode());
                if (isRedirect && status == HttpStatus.SC_TEMPORARY_REDIRECT) {
                    Header header = response.getFirstHeader("location"); // 跳转的目标地址是在 HTTP-HEAD上
                    String newuri = header.getValue();
                    HttpPost newHttpPost = new HttpPost(newuri);
                    newHttpPost.setHeader("Content-type", "application/json;charset=UTF-8");
                    if (bodyData != null && bodyData.size() > 0) {
                        newHttpPost.setEntity(
                                new StringEntity(
                                        objectMapper.writeValueAsString(bodyData), charset));
                    }
                    if (cookies != null && cookies.size() > 0) {
                        newHttpPost.addHeader("Cookie", getCookieFormat(cookies));
                    }

                    response = httpClient.execute(newHttpPost);
                    int newStatus = response.getStatusLine().getStatusCode();
                    if (newStatus == HttpStatus.SC_OK) {
                        responseBody = EntityUtils.toString(response.getEntity(), charset);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("url:{}--->http request error:", url, e);
            responseBody = null;
        } finally {
            if (HttpStatus.SC_OK != status && null != httpPost) {
                httpPost.abort();
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
        }
        return responseBody;
    }

    private static String getRequest(String url) throws IOException {
        return getRequest(url, (Map<String, Object>) null);
    }

    private static String getRequest(String url, Map<String, Object> cookies) throws IOException {
        Header[] headers = {};
        if (cookies != null && cookies.size() > 0) {
            Header header = new BasicHeader("Cookie", getCookieFormat(cookies));
            headers = new Header[] {header};
        }
        return getRequest(url, headers);
    }

    private static String getRequest(String url, Header[] headers) throws IOException {
        String respBody = null;
        HttpGet httpGet = null;
        CloseableHttpResponse response = null;
        int statusCode = 0;
        try {
            httpGet = new HttpGet(url);
            if (headers != null && headers.length > 0) {
                httpGet.setHeaders(headers);
            }
            response = httpClient.execute(httpGet);
            statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                respBody = EntityUtils.toString(entity, charset);
            } else if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
                throw new RuntimeException("登陆状态失效");
            } else {
                LOGGER.warn(
                        "request url:{} fail:{}", url, response.getStatusLine().getStatusCode());

                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    throw new RuntimeException(HttpStatus.SC_NOT_FOUND + "");
                } else if (response.getStatusLine().getStatusCode()
                        == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
                    throw new RuntimeException(HttpStatus.SC_INTERNAL_SERVER_ERROR + "");
                }
            }
        } catch (IOException e) {
            LOGGER.error("url:{}--->http request error:", url, e);
            throw e;
        } finally {
            if (HttpStatus.SC_OK != statusCode && null != httpGet) {
                httpGet.abort();
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
        }
        return respBody;
    }

    public static String get(String url) throws IOException {
        try {
            return get(url, null);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static String get(String url, Map<String, Object> cookies) throws IOException {
        try {
            return get(url, cookies, DEFAULT_RETRY_TIMES);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static String get(String url, Map<String, Object> cookies, int retryNumber)
            throws Exception {
        try {
            Header[] headers = {};
            if (cookies != null && cookies.size() > 0) {
                Header header = new BasicHeader("Cookie", getCookieFormat(cookies));
                headers = new Header[] {header};
            }
            return get(url, retryNumber, headers);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static String get(String url, int retryNumber, Header[] headers) throws Exception {
        return RetryUtil.executeWithRetry(
                new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return getRequest(url, headers);
                    }
                },
                retryNumber,
                SLEEP_TIME_MILLI_SECOND,
                false);
    }

    private static String getCookieFormat(Map<String, Object> cookies) {
        StringBuffer sb = new StringBuffer();
        Set<Map.Entry<String, Object>> sets = cookies.entrySet();
        for (Map.Entry<String, Object> s : sets) {
            sb.append(s.getKey()).append("=").append(s.getValue().toString()).append(";");
        }
        return sb.toString();
    }
}
