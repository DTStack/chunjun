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

package com.dtstack.chunjun.connector.selectdbcloud.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.selectdbcloud.common.LoadConstants;
import com.dtstack.chunjun.connector.selectdbcloud.converter.SelectdbcloudRawTypeMapper;
import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConfig;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SelectdbcloudSinkFactory extends SinkFactory {
    private final SelectdbcloudConfig conf;

    public SelectdbcloudSinkFactory(SyncConfig syncConf) {
        super(syncConf);
        conf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()),
                        SelectdbcloudConfig.class);
        conf.setFieldNames(
                conf.getColumn().stream().map(FieldConfig::getName).toArray(String[]::new));
        conf.setFieldDataTypes(
                conf.getColumn().stream()
                        .map(t -> SelectdbcloudRawTypeMapper.apply(t.getType()))
                        .toArray(DataType[]::new));
        setDefaults(conf);
        super.initCommonConf(conf);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return SelectdbcloudRawTypeMapper::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        SelectdbcloudOutputFormatBuilder builder =
                new SelectdbcloudOutputFormatBuilder(new SelectdbcloudOutputFormat()).setConf(conf);
        return createOutput(dataSet, builder.finish());
    }

    private void setDefaults(SelectdbcloudConfig conf) {

        if (conf.getMaxRetries() == null) {
            conf.setMaxRetries(LoadConstants.DEFAULT_MAX_RETRY_TIMES);
        }

        if (conf.getBatchSize() < 1) {
            conf.setBatchSize(LoadConstants.DEFAULT_BATCH_SIZE);
        }

        if (conf.getFlushIntervalMills() < 1L) {
            conf.setFlushIntervalMills(LoadConstants.DEFAULT_INTERVAL_MILLIS);
        }

        if (conf.getEnableDelete() == null) {
            conf.setEnableDelete(LoadConstants.DEFAULT_ENABLE_DELETE);
        }
    }
}
