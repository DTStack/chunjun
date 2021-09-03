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

package com.dtstack.flinkx.connector.emqx.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.emqx.conf.EmqxConf;
import com.dtstack.flinkx.connector.emqx.converter.EmqxColumnConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

/**
 * @author chuixue
 * @create 2021-06-01 20:08
 * @description
 */
public class EmqxSinkFactory extends SinkFactory {

    private final EmqxConf emqxConf;

    public EmqxSinkFactory(SyncConf syncConf) {
        super(syncConf);
        emqxConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()), EmqxConf.class);
        emqxConf.setColumn(syncConf.getReader().getFieldList());
        super.initFlinkxCommonConf(emqxConf);
        emqxConf.setParallelism(1);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("Emqx not support transform");
        }
        EmqxOutputFormatBuilder builder = new EmqxOutputFormatBuilder();
        builder.setEmqxConf(emqxConf);
        builder.setRowConverter(new EmqxColumnConverter(emqxConf));
        return createOutput(dataSet, builder.finish());
    }
}
