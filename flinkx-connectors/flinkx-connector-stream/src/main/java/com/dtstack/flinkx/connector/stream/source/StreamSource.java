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
package com.dtstack.flinkx.connector.stream.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.stream.conf.StreamConf;
import com.dtstack.flinkx.connector.stream.inputFormat.StreamInputFormatBuilder;
import com.dtstack.flinkx.source.BaseDataSource;
import com.dtstack.flinkx.util.GsonUtil;

/**
 * Date: 2021/04/07
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class StreamSource extends BaseDataSource {
    private StreamConf streamConf;

    public StreamSource(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        streamConf = GsonUtil.GSON.fromJson(GsonUtil.GSON.toJson(config.getReader().getParameter()), StreamConf.class);
        super.initFlinkxCommonConf(streamConf);
    }

    @Override
    public DataStream<Row> readData() {
        StreamInputFormatBuilder builder = new StreamInputFormatBuilder();
        builder.setStreamConf(streamConf);

        return createInput(builder.finish());
    }
}
