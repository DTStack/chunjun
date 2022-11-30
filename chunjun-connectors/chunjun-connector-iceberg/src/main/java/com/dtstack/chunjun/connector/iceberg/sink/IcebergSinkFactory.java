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

package com.dtstack.chunjun.connector.iceberg.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.iceberg.conf.IcebergWriterConf;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class IcebergSinkFactory extends SinkFactory {

    private final IcebergWriterConf writerConf;

    public IcebergSinkFactory(SyncConfig config) {
        super(config);
        writerConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()),
                        IcebergWriterConf.class);
        writerConf.setColumn(config.getWriter().getFieldList());
        super.initCommonConf(writerConf);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("iceberg not support transform");
        }
        return createOutput(dataSet, null);
    }

    @Override
    protected DataStreamSink<RowData> createOutput(
            DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    @Override
    protected DataStreamSink<RowData> createOutput(
            DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat, String sinkName) {
        Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);

        // 初始化 hadoop conf
        Configuration conf =
                FileSystemUtil.getConfiguration(
                        writerConf.getHadoopConfig(), writerConf.getDefaultFS());

        TableLoader tableLoader = TableLoader.fromHadoopTable(writerConf.getPath(), conf);
        SingleOutputStreamOperator<RowData> streamOperator =
                dataSet.map(new IcebergMetricsMapFunction(writerConf));
        DataStreamSink<RowData> dataDataStreamSink = null;
        // 判断写出模式
        String writeMode = writerConf.getWriteMode();
        if (writeMode.equals(IcebergWriterConf.UPSERT_WRITE_MODE)) {
            dataDataStreamSink =
                    FlinkSink.forRowData(streamOperator)
                            .tableLoader(tableLoader)
                            .equalityFieldColumns(Lists.newArrayList("id"))
                            .upsert(true)
                            .build();
        } else if (writeMode.equals(IcebergWriterConf.OVERWRITE_WRITE_MODE)) {
            dataDataStreamSink =
                    FlinkSink.forRowData(streamOperator)
                            .tableLoader(tableLoader)
                            .overwrite(true)
                            .build();

        } else if (writeMode.equals(IcebergWriterConf.APPEND_WRITE_MODE)) {
            dataDataStreamSink =
                    FlinkSink.forRowData(streamOperator).tableLoader(tableLoader).build();
        } else {
            throw new UnsupportedOperationException("iceberg not support writeMode :" + writeMode);
        }

        dataDataStreamSink.name(sinkName);
        return dataDataStreamSink;
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
