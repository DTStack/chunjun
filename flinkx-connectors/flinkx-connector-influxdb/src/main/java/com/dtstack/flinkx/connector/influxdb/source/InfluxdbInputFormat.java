/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one
 *  *  * or more contributor license agreements.  See the NOTICE file
 *  *  * distributed with this work for additional information
 *  *  * regarding copyright ownership.  The ASF licenses this file
 *  *  * to you under the Apache License, Version 2.0 (the
 *  *  * "License"); you may not use this file except in compliance
 *  *  * with the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package com.dtstack.flinkx.connector.influxdb.source;

import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbConfig;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/3/8
 */
public class InfluxdbInputFormat extends BaseRichInputFormat {

    private InfluxdbConfig config;
    private InfluxDB influxDB;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        return new InputSplit[0];
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {

    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        RowData data;
        try {
            data = rowConverter.toInternal(null);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
        return data;
    }

    @Override
    protected void closeInternal() throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }


    public InfluxDB getConnection() {
        if (influxDB == null) {
            OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                    .connectTimeout(15000, TimeUnit.MILLISECONDS)
                    .readTimeout(this.config.getQueryTimeOut(), TimeUnit.SECONDS);
            InfluxDB.ResponseFormat format = InfluxDB.ResponseFormat.valueOf(this.config.getFormat());
            clientBuilder.addInterceptor(new Interceptor() {
                @NotNull
                @Override
                public Response intercept(@NotNull Chain chain) throws IOException {
                    Request request = chain.request();
                    HttpUrl httpUrl = request.url()
                            .newBuilder()
                            // add common parameter
                            .addQueryParameter("epoch",
                                    config.getEpoch().toLowerCase(Locale.ENGLISH))
                            .build();
                    Request build = request.newBuilder()
                            .url(httpUrl)
                            .build();
                    Response response = chain.proceed(build);
                    return response;
                }
            });
            influxDB = InfluxDBFactory.connect(
                    this.config.getUrl(),
                    this.config.getUsername(),
                    this.config.getPassword(),
                    clientBuilder,
                    format);
        }
        return influxDB;
    }

    public void setConfig(InfluxdbConfig config) {
        this.config = config;
    }
}
