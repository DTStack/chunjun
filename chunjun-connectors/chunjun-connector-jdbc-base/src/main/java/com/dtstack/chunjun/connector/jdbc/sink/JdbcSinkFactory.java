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

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.conf.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.table.options.SinkOptions;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class JdbcSinkFactory extends SinkFactory {

    protected JdbcConf jdbcConf;
    protected JdbcDialect jdbcDialect;

    protected List<String> columnNameList;
    protected List<String> columnTypeList;

    public JdbcSinkFactory(SyncConf syncConf, JdbcDialect jdbcDialect) {
        super(syncConf);
        this.jdbcDialect = jdbcDialect;
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConf.class, new ConnectionAdapter("SinkConnectionConf"))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        jdbcConf = gson.fromJson(gson.toJson(syncConf.getWriter().getParameter()), getConfClass());
        int batchSize =
                syncConf.getWriter()
                        .getIntVal(
                                "batchSize", SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS.defaultValue());
        jdbcConf.setBatchSize(batchSize);
        long flushIntervalMills =
                syncConf.getWriter()
                        .getLongVal(
                                "flushIntervalMills",
                                SinkOptions.SINK_BUFFER_FLUSH_INTERVAL.defaultValue());
        jdbcConf.setFlushIntervalMills(flushIntervalMills);
        jdbcConf.setColumn(syncConf.getWriter().getFieldList());
        Properties properties = syncConf.getWriter().getProperties("properties", null);
        jdbcConf.setProperties(properties);
        if (StringUtils.isNotEmpty(syncConf.getWriter().getSemantic())) {
            jdbcConf.setSemantic(syncConf.getWriter().getSemantic());
        }
        super.initCommonConf(jdbcConf);
        resetTableInfo();
        rebuildJdbcConf(jdbcConf);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        JdbcOutputFormatBuilder builder = getBuilder();
        initColumnInfo();
        builder.setJdbcConf(jdbcConf);
        builder.setDdlConf(ddlConf);
        builder.setJdbcDialect(jdbcDialect);
        builder.setColumnNameList(columnNameList);
        builder.setColumnTypeList(columnTypeList);

        AbstractRowConverter rowConverter;
        AbstractRowConverter keyRowConverter;
        final RowType rowType =
                TableUtil.createRowType(jdbcConf.getColumn(), getRawTypeConverter());
        final RowType keyRowType =
                TableUtil.createRowType(
                        jdbcConf.getColumn().stream()
                                .filter(field -> jdbcConf.getUniqueKey().contains(field.getName()))
                                .collect(Collectors.toList()),
                        getRawTypeConverter());
        // 同步任务使用transform
        if (!useAbstractBaseColumn) {
            rowConverter = jdbcDialect.getRowConverter(rowType);
            keyRowConverter = jdbcDialect.getRowConverter(keyRowType);
        } else {
            rowConverter = jdbcDialect.getColumnConverter(rowType, jdbcConf);
            keyRowConverter = jdbcDialect.getColumnConverter(keyRowType, jdbcConf);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        builder.setKeyRowType(keyRowType);
        builder.setKeyRowConverter(keyRowConverter);

        return createOutput(dataSet, builder.finish());
    }

    protected void initColumnInfo() {
        Connection conn = getConn();
        Pair<List<String>, List<String>> tableMetaData = getTableMetaData(conn);
        Pair<List<String>, List<String>> selectedColumnInfo =
                JdbcUtil.buildColumnWithMeta(jdbcConf, tableMetaData, null);
        columnNameList = selectedColumnInfo.getLeft();
        columnTypeList = selectedColumnInfo.getRight();
        JdbcUtil.closeDbResources(null, null, conn, false);
    }

    protected Pair<List<String>, List<String>> getTableMetaData(Connection dbConn) {
        Tuple3<String, String, String> tableIdentify =
                jdbcDialect.getTableIdentify(jdbcConf.getSchema(), jdbcConf.getTable());
        return JdbcUtil.getTableMetaData(
                tableIdentify.f0, tableIdentify.f1, tableIdentify.f2, dbConn);
    }

    protected Connection getConn() {
        return JdbcUtil.getConnection(jdbcConf, jdbcDialect);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return jdbcDialect.getRawTypeConverter();
    }

    protected Class<? extends JdbcConf> getConfClass() {
        return JdbcConf.class;
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
        if (StringUtils.isBlank(jdbcConf.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConf, "\\\"", "\\\"");
        }
    }

    protected void rebuildJdbcConf(JdbcConf jdbcConf) {
        // updateKey has Deprecated，please use uniqueKey
        if (MapUtils.isNotEmpty(jdbcConf.getUpdateKey())
                && CollectionUtils.isEmpty(jdbcConf.getUniqueKey())) {
            for (Map.Entry<String, List<String>> entry : jdbcConf.getUpdateKey().entrySet()) {
                if (CollectionUtils.isNotEmpty(entry.getValue())) {
                    jdbcConf.setUniqueKey(entry.getValue());
                    break;
                }
            }
        }
    }
}
