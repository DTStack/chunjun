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

import org.apache.flink.hadoop.shaded.org.apache.http.HttpEntity;
import org.apache.flink.hadoop.shaded.org.apache.http.HttpStatus;
import org.apache.flink.hadoop.shaded.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.flink.hadoop.shaded.org.apache.http.client.methods.HttpGet;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.util.EntityUtils;

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;

/**
 * @author jiangbo
 * @date 2018/7/10 14:08
 */
public class URLUtil {

    private static int MAX_RETRY_TIMES = 3;

    private static int SLEEP_TIME_MILLI_SECOND = 2000;

    private static Charset charset = Charset.forName("UTF-8");

    public static InputStream open(String url) throws Exception{
        return RetryUtil.executeWithRetry(new Callable<InputStream>() {
            @Override
            public InputStream call() throws Exception{
                return new URL(url).openStream();
            }
        },MAX_RETRY_TIMES,SLEEP_TIME_MILLI_SECOND,false);
    }

    public static String get(CloseableHttpClient httpClient, String url) throws Exception{
        return RetryUtil.executeWithRetry(new Callable<String>() {
            @Override
            public String call() throws Exception{
                String respBody = null;
                HttpGet httpGet = new HttpGet(url);
                CloseableHttpResponse response = httpClient.execute(httpGet);

                if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
                    HttpEntity entity = response.getEntity();
                    respBody = EntityUtils.toString(entity,charset);
                }

                return respBody;
            }
        },MAX_RETRY_TIMES,SLEEP_TIME_MILLI_SECOND,false);
    }
}
