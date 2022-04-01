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

package com.dtstack.flinkx.connector.jdbc.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.flinkx.connector.jdbc.conf.ConnectionConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * The Reader plugin for any database that can be connected via JDBC.
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public abstract class JdbcSourceFactory extends SourceFactory {

    private static final int DEFAULT_FETCH_SIZE = 1024;
    private static final int DEFAULT_QUERY_TIMEOUT = 300;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 600;
    protected JdbcConf jdbcConf;
    protected JdbcDialect jdbcDialect;

    public JdbcSourceFactory(
            SyncConf syncConf, StreamExecutionEnvironment env, JdbcDialect jdbcDialect) {
        super(syncConf, env);
        this.jdbcDialect = jdbcDialect;
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConf.class, new ConnectionAdapter("SourceConnectionConf"))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        jdbcConf = gson.fromJson(gson.toJson(syncConf.getReader().getParameter()), getConfClass());
        if (StringUtils.isBlank(jdbcConf.getIncreColumn())) jdbcConf.setPolling(false);
        jdbcConf.setColumn(syncConf.getReader().getFieldList());

        Properties properties = syncConf.getWriter().getProperties("properties", null);
        jdbcConf.setProperties(properties);
        String name = syncConf.getRestore().getRestoreColumnName();
        if (StringUtils.isNotBlank(name)) {
            FieldConf fieldConf = FieldConf.getSameNameMetaColumn(jdbcConf.getColumn(), name);
            if (fieldConf != null) {
                jdbcConf.setRestoreColumn(name);
                jdbcConf.setRestoreColumnIndex(fieldConf.getIndex());
                jdbcConf.setRestoreColumnType(fieldConf.getType());
            } else {
                throw new IllegalArgumentException("unknown restore column name: " + name);
            }
        }
        initIncrementConfig(jdbcConf);
        super.initFlinkxCommonConf(jdbcConf);
        if(StringUtils.isBlank(jdbcConf.getCustomSql())){
            rebuildJdbcConf();
        }
    }

    protected Class<? extends JdbcConf> getConfClass() {
        return JdbcConf.class;
    }

    @Override
    public DataStream<RowData> createSource() {
        JdbcInputFormatBuilder builder = getBuilder();

        int fetchSize = jdbcConf.getFetchSize();
        jdbcConf.setFetchSize(fetchSize == 0 ? getDefaultFetchSize() : fetchSize);

        int queryTimeOut = jdbcConf.getQueryTimeOut();
        jdbcConf.setQueryTimeOut(queryTimeOut == 0 ? DEFAULT_QUERY_TIMEOUT : queryTimeOut);

        int connectTimeOut = jdbcConf.getConnectTimeOut();
        jdbcConf.setConnectTimeOut(
                connectTimeOut == 0 ? DEFAULT_CONNECTION_TIMEOUT : connectTimeOut);

        builder.setJdbcConf(jdbcConf);
        builder.setJdbcDialect(jdbcDialect);

        AbstractRowConverter rowConverter = null;
        if (!useAbstractBaseColumn) {
            checkConstant(jdbcConf);
            final RowType rowType =
                    TableUtil.createRowType(jdbcConf.getColumn(), getRawTypeConverter());
            rowConverter = jdbcDialect.getRowConverter(rowType);
        }
        builder.setRowConverter(rowConverter);

        return createInput(builder.finish());
    }

    /**
     * 获取JDBC插件的具体inputFormatBuilder
     *
     * @return JdbcInputFormatBuilder
     */
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new JdbcInputFormat());
    }

    /**
     * 初始化增量或间隔轮询任务配置
     *
     * @param jdbcConf jdbcConf
     */
    private void initIncrementConfig(JdbcConf jdbcConf) {
        String increColumn = jdbcConf.getIncreColumn();

        // 增量字段不为空，表示任务为增量或间隔轮询任务
        if (StringUtils.isNotBlank(increColumn)) {
            List<FieldConf> fieldConfList = jdbcConf.getColumn();
            String type = null;
            String name = null;
            int index = -1;

            // 纯数字则表示增量字段在column中的顺序位置
            if (NumberUtils.isNumber(increColumn)) {
                int idx = Integer.parseInt(increColumn);
                if (idx > fieldConfList.size() - 1) {
                    throw new FlinkxRuntimeException(
                            String.format(
                                    "config error : incrementColumn must less than column.size() when increColumn is number, column = %s, size = %s, increColumn = %s",
                                    GsonUtil.GSON.toJson(fieldConfList),
                                    fieldConfList.size(),
                                    increColumn));
                }
                FieldConf fieldColumn = fieldConfList.get(idx);
                type = fieldColumn.getType();
                name = fieldColumn.getName();
                index = fieldColumn.getIndex();
            } else {
                for (FieldConf field : fieldConfList) {
                    if (Objects.equals(increColumn, field.getName())) {
                        type = field.getType();
                        name = field.getName();
                        index = field.getIndex();
                        break;
                    }
                }
            }
            if (type == null || name == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "config error : increColumn's name or type is null, column = %s, increColumn = %s",
                                GsonUtil.GSON.toJson(fieldConfList), increColumn));
            }

            jdbcConf.setIncrement(true);
            jdbcConf.setIncreColumn(name);
            jdbcConf.setIncreColumnType(type);
            jdbcConf.setIncreColumnIndex(index);

            jdbcConf.setRestoreColumn(name);
            jdbcConf.setRestoreColumnType(type);
            jdbcConf.setRestoreColumnIndex(index);
        }
    }

    protected int getDefaultFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return jdbcDialect.getRawTypeConverter();
    }

    protected void rebuildJdbcConf() {
        // table字段有可能是schema.table格式 需要转换为对应的schema 和 table 字段
        if (StringUtils.isBlank(jdbcConf.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConf, "\\\"", "\\\"");
        }
    }
}
