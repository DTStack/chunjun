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
package com.dtstack.flinkx.connectors.stream.sink;

import com.dtstack.flinkx.conf.FlinkxConf;
import com.dtstack.flinkx.connectors.stream.conf.StreamConf;
import com.dtstack.flinkx.connectors.stream.outputFormat.StreamOutputFormatBuilder;
import com.dtstack.flinkx.sink.BaseDataSink;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

/**
 * Date: 2021/04/07
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class StreamSink extends BaseDataSink {
    private StreamConf streamConf;

    public StreamSink(FlinkxConf config) {
        super(config);
        streamConf = GsonUtil.GSON.fromJson(GsonUtil.GSON.toJson(config.getWriter().getParameter()), StreamConf.class);
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> writeData(DataStream<Tuple2<Boolean, Row>> dataSet) {
        StreamOutputFormatBuilder builder = new StreamOutputFormatBuilder();
        builder.setConfig(config);
        builder.setStreamConf(streamConf);

        return createOutput(dataSet, builder.finish());
    }
}
