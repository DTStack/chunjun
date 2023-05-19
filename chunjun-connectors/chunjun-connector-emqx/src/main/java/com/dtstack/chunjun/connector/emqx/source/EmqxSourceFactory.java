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

package com.dtstack.chunjun.connector.emqx.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.emqx.config.EmqxConfig;
import com.dtstack.chunjun.connector.emqx.converter.EmqxSyncConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

public class EmqxSourceFactory extends SourceFactory {

    private final EmqxConfig emqxConfig;

    public EmqxSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);
        emqxConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getReader().getParameter()), EmqxConfig.class);
        emqxConfig.setColumn(syncConfig.getReader().getFieldList());
        super.initCommonConf(emqxConfig);
        emqxConfig.setParallelism(1);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return null;
    }

    @Override
    public DataStream<RowData> createSource() {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("Emqx not support transform");
        }
        EmqxInputFormatBuilder builder = new EmqxInputFormatBuilder();
        builder.setEmqxConf(emqxConfig);
        builder.setRowConverter(new EmqxSyncConverter(emqxConfig), useAbstractBaseColumn);
        return createInput(builder.finish());
    }
}
