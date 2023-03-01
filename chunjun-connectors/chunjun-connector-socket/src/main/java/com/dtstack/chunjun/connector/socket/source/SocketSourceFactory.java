/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.socket.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.socket.entity.SocketConfig;
import com.dtstack.chunjun.connector.socket.inputformat.SocketInputFormatBuilder;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class SocketSourceFactory extends SourceFactory {

    private final SocketConfig socketConfig;

    public SocketSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        socketConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()), SocketConfig.class);
        super.initCommonConf(socketConfig);
    }

    @Override
    public DataStream<RowData> createSource() {
        SocketInputFormatBuilder builder = new SocketInputFormatBuilder();
        builder.setSocketConfig(socketConfig);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
