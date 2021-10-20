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

package com.dtstack.flinkx.connector.http.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.http.common.HttpWriterConfig;
import com.dtstack.flinkx.connector.http.outputformat.HttpOutputFormatBuilder;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Date: 2021/04/13 Company: www.dtstack.com
 *
 * @author shifang
 */
public class HttpSinkFactory extends SinkFactory {

    protected static final Logger LOG = LoggerFactory.getLogger(HttpSinkFactory.class);

    protected HttpWriterConfig httpWriterConfig;

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }

    public HttpSinkFactory(SyncConf syncConf) {
        super(syncConf);
        httpWriterConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()),
                        HttpWriterConfig.class);
        super.initFlinkxCommonConf(httpWriterConfig);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HttpOutputFormatBuilder builder = getBuilder();
        builder.setConfig(httpWriterConfig);
        return createOutput(dataSet, builder.finish());
    }

    /**
     * 获取JDBC插件的具体outputFormatBuilder
     *
     * @return JdbcOutputFormatBuilder
     */
    protected HttpOutputFormatBuilder getBuilder() {
        return new HttpOutputFormatBuilder();
    };
}
