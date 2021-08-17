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

package com.dtstack.flinkx.connector.restapi.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.restapi.common.RestapiKeys;
import com.dtstack.flinkx.connector.restapi.common.RestapiWriterConfig;
import com.dtstack.flinkx.connector.restapi.outputformat.RestapiOutputFormatBuilder;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

/**
 * Date: 2021/04/13 Company: www.dtstack.com
 *
 * @author shifang
 */
public class RestapiSinkFactory extends SinkFactory {

    protected static final Logger LOG = LoggerFactory.getLogger(RestapiSinkFactory.class);

    protected RestapiWriterConfig restapiWriterConfig;

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }

    public RestapiSinkFactory(SyncConf syncConf) {
        super(syncConf);
        restapiWriterConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()),
                        RestapiWriterConfig.class);
        Object tempObj;

        tempObj = syncConf.getWriter().getParameter().get(RestapiKeys.KEY_HEADER);
        if (tempObj != null) {
            for (Map<String, String> map : (ArrayList<Map<String, String>>) tempObj) {
                restapiWriterConfig.getFormatHeader().putAll(map);
            }
        }

        tempObj = syncConf.getWriter().getParameter().get(RestapiKeys.KEY_BODY);
        if (tempObj != null) {
            for (Map<String, Object> map : (ArrayList<Map<String, Object>>) tempObj) {
                restapiWriterConfig.getFormatBody().putAll(map);
            }
        }
        tempObj = syncConf.getWriter().getParameter().get(RestapiKeys.KEY_PARAMS);
        if (tempObj != null) {
            restapiWriterConfig.getParams().putAll((Map) tempObj);
        }
        super.initFlinkxCommonConf(restapiWriterConfig);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        RestapiOutputFormatBuilder builder = getBuilder();
        builder.setConfig(restapiWriterConfig);
        return createOutput(dataSet, builder.finish());
    }

    /**
     * 获取JDBC插件的具体outputFormatBuilder
     *
     * @return JdbcOutputFormatBuilder
     */
    protected RestapiOutputFormatBuilder getBuilder() {
        return new RestapiOutputFormatBuilder();
    };
}
