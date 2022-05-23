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
package com.dtstack.chunjun.connector.starrocks.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.conf.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRawTypeConverter;
import com.dtstack.chunjun.connector.starrocks.dialect.StarRocksDialect;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.connector.flink.row.StarRocksTableRowTransformer;
import com.starrocks.connector.flink.table.StarRocksDynamicSinkFunction;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * @author lihongwei
 * @date 2022/04/11
 */
public class StarrocksSinkFactory extends JdbcSinkFactory {

    private final StarRocksConf starRocksConf;
    protected TypeInformation<RowData> typeInformation;

    public StarrocksSinkFactory(SyncConf syncConf) {
        super(syncConf, new StarRocksDialect());
        JdbcUtil.putExtParam(jdbcConf);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConf.class, new ConnectionAdapter("SinkConnectionConf"))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        starRocksConf =
                gson.fromJson(
                        gson.toJson(syncConf.getWriter().getParameter()), StarRocksConf.class);
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
        List<FieldConf> fields = jdbcConf.getColumn();
        String[] fieldNames = fields.stream().map(FieldConf::getName).toArray(String[]::new);
        String[] fieldTypeList = fields.stream().map(FieldConf::getType).toArray(String[]::new);
        DataType[] fieldTypes = new DataType[fieldNames.length];
        List<String> primaryKeyList = starRocksConf.getPrimaryKey();
        for (int i = 0; i < fieldNames.length; i++) {
            if (primaryKeyList.contains(fieldNames[i])) {
                fieldTypes[i] = StarRocksRawTypeConverter.apply(fieldTypeList[i]).notNull();
            } else {
                fieldTypes[i] = StarRocksRawTypeConverter.apply(fieldTypeList[i]);
            }
        }
        StarRocksTableRowTransformer starRocksGenericRowTransformer =
                new StarRocksTableRowTransformer(typeInformation);
        StarRocksDynamicSinkFunction starRocksDynamicSinkFunction =
                new StarRocksDynamicSinkFunction(
                        getStarRocksSinkOptions(fieldNames),
                        primaryKeyList.size() > 0
                                ? TableSchema.builder()
                                        .primaryKey(
                                                primaryKeyList.toArray(
                                                        new String[primaryKeyList.size()]))
                                        .fields(fieldNames, fieldTypes)
                                        .build()
                                : TableSchema.builder().fields(fieldNames, fieldTypes).build(),
                        starRocksGenericRowTransformer);
        DataStreamSink<RowData> dataStreamSink = dataSet.addSink(starRocksDynamicSinkFunction);
        dataStreamSink.name(sinkName);
        return dataStreamSink;
    }

    public StarRocksSinkOptions getStarRocksSinkOptions(String[] fieldNames) {
        return StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcConf.getJdbcUrl())
                .withProperty("load-url", starRocksConf.getLoadUrl())
                .withProperty("username", jdbcConf.getUsername())
                .withProperty("password", jdbcConf.getPassword())
                .withProperty("table-name", jdbcConf.getTable())
                .withProperty("database-name", jdbcConf.getSchema())
                // in case of raw data contains common delimiter like '\n'
                // .withProperty("sink.properties.row_delimiter","\\x02")
                // in case of raw data contains common separator like '\t'
                // .withProperty("sink.properties.column_separator","\\x01")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.buffer-flush.interval-ms", "2000")
                .withProperty("sink.properties.columns", StringUtils.join(fieldNames, ","))
                .build();
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {

        JdbcOutputFormatBuilder builder = getBuilder();

        int connectTimeOut = jdbcConf.getConnectTimeOut();
        jdbcConf.setConnectTimeOut(connectTimeOut == 0 ? 600 : connectTimeOut);

        builder.setJdbcConf(jdbcConf);
        builder.setJdbcDialect(jdbcDialect);

        AbstractRowConverter rowConverter = null;
        // 同步任务使用transform
        if (!useAbstractBaseColumn) {
            final RowType rowType =
                    TableUtil.createRowType(jdbcConf.getColumn(), getRawTypeConverter());
            rowConverter = jdbcDialect.getRowConverter(rowType);
            typeInformation =
                    TableUtil.getTypeInformation(Collections.emptyList(), getRawTypeConverter());
        } else {
            List<FieldConf> fieldList = syncConf.getWriter().getFieldList();
            typeInformation = TableUtil.getTypeInformation(fieldList, getRawTypeConverter());
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);

        builder.finish().initializeGlobal(1);

        return createOutput(dataSet, null);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return StarRocksRawTypeConverter::apply;
    }
}
