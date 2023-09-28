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

package com.dtstack.chunjun.connector.arctic.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.arctic.config.ArcticWriterConfig;
import com.dtstack.chunjun.connector.arctic.converter.ArcticRawTypeMapper;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import com.netease.arctic.ams.api.client.ArcticThriftUrl;
import com.netease.arctic.flink.InternalCatalogBuilder;
import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.flink.write.FlinkSink;
import com.netease.arctic.table.TableIdentifier;

public class ArcticSinkFactory extends SinkFactory {

    private final ArcticWriterConfig writerConf;

    public ArcticSinkFactory(SyncConfig config) {
        super(config);
        writerConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()),
                        ArcticWriterConfig.class);
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

        ArcticThriftUrl arcticThriftUrl = ArcticThriftUrl.parse(writerConf.getAmsUrl());
        InternalCatalogBuilder catalogBuilder =
                InternalCatalogBuilder.builder().metastoreUrl(arcticThriftUrl.url());
        TableIdentifier TABLE_ID =
                TableIdentifier.of(
                        arcticThriftUrl.catalogName(),
                        writerConf.getDatabaseName(),
                        writerConf.getTableName());
        ArcticTableLoader tableLoader = ArcticTableLoader.of(TABLE_ID, catalogBuilder);

        TableSchema FLINK_SCHEMA =
                TableSchema.fromResolvedSchema(
                        TableUtil.createTableSchema(
                                writerConf.getColumn(), ArcticRawTypeMapper::apply));

        SingleOutputStreamOperator<RowData> streamOperator =
                dataSet.map(new ArcticMetricsMapFunction(writerConf));
        DataStreamSink<RowData> dataDataStreamSink;

        String tableMode = writerConf.getTableMode();
        if (tableMode.equalsIgnoreCase(ArcticWriterConfig.UNKEYED_TABLE_MODE)
                && writerConf.isOverwrite()) {
            dataDataStreamSink =
                    (DataStreamSink<RowData>)
                            FlinkSink.forRowData(streamOperator)
                                    .tableLoader(tableLoader)
                                    .overwrite(true)
                                    .flinkSchema(FLINK_SCHEMA)
                                    .build();
        } else if ((tableMode.equalsIgnoreCase(ArcticWriterConfig.UNKEYED_TABLE_MODE)
                        && !writerConf.isOverwrite())
                || tableMode.equalsIgnoreCase(ArcticWriterConfig.KEYED_TABLE_MODE)) {
            dataDataStreamSink =
                    (DataStreamSink<RowData>)
                            FlinkSink.forRowData(streamOperator)
                                    .tableLoader(tableLoader)
                                    .flinkSchema(FLINK_SCHEMA)
                                    .build();
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "arctic not support mode, tableMode|isOverwrite : %s|%s",
                            tableMode, writerConf.isOverwrite()));
        }

        dataDataStreamSink.name(sinkName);
        return dataDataStreamSink;
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return ArcticRawTypeMapper::apply;
    }
}
