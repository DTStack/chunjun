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

package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.config.ConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.SinkConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.table.options.SinkOptions;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class JdbcSinkFactory extends SinkFactory {

    protected JdbcConfig jdbcConfig;
    protected JdbcDialect jdbcDialect;

    protected List<String> columnNameList;
    protected List<TypeConfig> columnTypeList;

    public JdbcSinkFactory(SyncConfig syncConfig, JdbcDialect jdbcDialect) {
        super(syncConfig);
        this.jdbcDialect = jdbcDialect;
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConfig.class,
                                new ConnectionAdapter(SinkConnectionConfig.class.getName()))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        jdbcConfig =
                gson.fromJson(gson.toJson(syncConfig.getWriter().getParameter()), getConfClass());
        int batchSize =
                syncConfig
                        .getWriter()
                        .getIntVal(
                                "batchSize", SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS.defaultValue());
        jdbcConfig.setBatchSize(batchSize);
        long flushIntervalMills =
                syncConfig
                        .getWriter()
                        .getLongVal(
                                "flushIntervalMills",
                                SinkOptions.SINK_BUFFER_FLUSH_INTERVAL.defaultValue());
        jdbcConfig.setFlushIntervalMills(flushIntervalMills);
        jdbcConfig.setColumn(syncConfig.getWriter().getFieldList());
        Properties properties = syncConfig.getWriter().getProperties("properties", null);
        jdbcConfig.setProperties(properties);
        if (StringUtils.isNotEmpty(syncConfig.getWriter().getSemantic())) {
            jdbcConfig.setSemantic(syncConfig.getWriter().getSemantic());
        }
        super.initCommonConf(jdbcConfig);
        resetTableInfo();
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        JdbcOutputFormatBuilder builder = getBuilder();
        initColumnInfo();
        builder.setJdbcConf(jdbcConfig);
        builder.setDdlConfig(ddlConfig);
        builder.setJdbcDialect(jdbcDialect);
        builder.setColumnNameList(columnNameList);
        builder.setColumnTypeList(columnTypeList);

        AbstractRowConverter<?, ?, ?, ?> rowConverter;
        AbstractRowConverter keyRowConverter;
        final RowType rowType = TableUtil.createRowType(jdbcConfig.getColumn(), getRawTypeMapper());
        final RowType keyRowType =
                TableUtil.createRowType(
                        jdbcConfig.getColumn().stream()
                                .filter(
                                        field ->
                                                jdbcConfig.getUniqueKey().contains(field.getName()))
                                .collect(Collectors.toList()),
                        getRawTypeMapper());
        // 同步任务使用transform
        if (!useAbstractBaseColumn) {
            rowConverter = jdbcDialect.getRowConverter(rowType);
            keyRowConverter = jdbcDialect.getRowConverter(keyRowType);
        } else {
            rowConverter = jdbcDialect.getColumnConverter(rowType, jdbcConfig);
            keyRowConverter = jdbcDialect.getColumnConverter(keyRowType, jdbcConfig);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        builder.setKeyRowType(keyRowType);
        builder.setKeyRowConverter(keyRowConverter);

        return createOutput(dataSet, builder.finish());
    }

    protected void initColumnInfo() {
        Connection conn = getConn();
        Pair<List<String>, List<TypeConfig>> tableMetaData = getTableMetaData(conn);
        Pair<List<String>, List<TypeConfig>> selectedColumnInfo =
                JdbcUtil.buildColumnWithMeta(jdbcConfig, tableMetaData, null);
        columnNameList = selectedColumnInfo.getLeft();
        columnTypeList = selectedColumnInfo.getRight();
        JdbcUtil.closeDbResources(null, null, conn, false);
    }

    protected Pair<List<String>, List<TypeConfig>> getTableMetaData(Connection dbConn) {
        return jdbcDialect.getTableMetaData(dbConn, jdbcConfig);
    }

    protected Connection getConn() {
        return JdbcUtil.getConnection(jdbcConfig, jdbcDialect);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return jdbcDialect.getRawTypeConverter();
    }

    protected Class<? extends JdbcConfig> getConfClass() {
        return JdbcConfig.class;
    }

    /**
     * 获取JDBC插件的具体outputFormatBuilder
     *
     * @return JdbcOutputFormatBuilder
     */
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new JdbcOutputFormat());
    }

    /** table字段有可能是schema.table格式 需要转换为对应的schema 和 table 字段* */
    protected void resetTableInfo() {
        if (StringUtils.isBlank(jdbcConfig.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\\"", "\\\"");
        }
    }
}
