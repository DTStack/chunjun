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

package com.dtstack.flinkx.connector.stream.sink;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

import com.dtstack.flinkx.connector.stream.conf.StreamSinkConf;
import com.dtstack.flinkx.connector.stream.outputFormat.StreamOutputFormatBuilder;

/**
 * @author chuixue
 * @create 2021-04-09 09:20
 * @description SinkFunction的包装类DynamicTableSink
 **/

public class StreamDynamicTableSink implements DynamicTableSink {
    private final StreamSinkConf streamSinkConf;

    public StreamDynamicTableSink(StreamSinkConf streamSinkConf) {
        this.streamSinkConf = streamSinkConf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {

        // 一些其他参数的封装
        streamSinkConf.setPrint(false);

        StreamOutputFormatBuilder builder = StreamOutputFormatBuilder
                .builder()
                .setStreamSinkConf(streamSinkConf)
                .setConverter(context.createDataStructureConverter(streamSinkConf.getType()));

        return SinkFunctionProvider.of(new StreamSinkFunction(builder.finish()), 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new StreamDynamicTableSink(streamSinkConf);
    }

    @Override
    public String asSummaryString() {
        return "StreamDynamicTableSink";
    }
}
