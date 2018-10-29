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

package com.dtstack.flinkx.stream.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

/**
 * This write plugin is used to test the performance of the read plugin, and the plugin directly discards the read data.
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class StreamWriter extends DataWriter {

    protected boolean print;

    public StreamWriter(DataTransferConfig config) {
        super(config);
        print = config.getJob().getContent().get(0).getWriter().getParameter().getBooleanVal("print",false);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        StreamOutputFormatBuilder builder = new StreamOutputFormatBuilder();
        builder.setPrint(print);

        OutputFormatSinkFunction formatSinkFunction = new OutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(formatSinkFunction);
        dataStreamSink.name("streamwriter");

        return dataStreamSink;
    }
}
