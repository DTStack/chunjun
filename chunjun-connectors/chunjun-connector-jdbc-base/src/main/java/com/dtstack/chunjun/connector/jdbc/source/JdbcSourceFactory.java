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

package com.dtstack.chunjun.connector.jdbc.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.config.ConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.SourceConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.KeyUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.NumericTypeUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/** The Reader plugin for any database that can be connected via JDBC. */
public abstract class JdbcSourceFactory extends SourceFactory {

    private static final int DEFAULT_FETCH_SIZE = 1024;
    private static final int DEFAULT_QUERY_TIMEOUT = 300;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 600;
    protected JdbcConfig jdbcConfig;
    protected JdbcDialect jdbcDialect;

    protected KeyUtil<?, BigInteger> incrementKeyUtil = new NumericTypeUtil();
    protected KeyUtil<?, BigInteger> splitKeyUtil = new NumericTypeUtil();
    protected KeyUtil<?, BigInteger> restoreKeyUtil = new NumericTypeUtil();
    protected List<String> columnNameList;
    protected List<TypeConfig> columnTypeList;

    public JdbcSourceFactory(
            SyncConfig syncConfig, StreamExecutionEnvironment env, JdbcDialect jdbcDialect) {
        super(syncConfig, env);
        this.jdbcDialect = jdbcDialect;
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConfig.class,
                                new ConnectionAdapter(SourceConnectionConfig.class.getName()))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        jdbcConfig =
                gson.fromJson(gson.toJson(syncConfig.getReader().getParameter()), getConfClass());
        if (StringUtils.isBlank(jdbcConfig.getIncreColumn())) jdbcConfig.setPolling(false);
        jdbcConfig.setColumn(syncConfig.getReader().getFieldList());

        Properties properties = syncConfig.getReader().getProperties("properties", null);
        jdbcConfig.setProperties(properties);

        setDefaultSplitStrategy(jdbcConfig);
        super.initCommonConf(jdbcConfig);
        if (StringUtils.isBlank(jdbcConfig.getCustomSql())) {
            rebuildJdbcConf();
        }
    }

    protected Class<? extends JdbcConfig> getConfClass() {
        return JdbcConfig.class;
    }

    @Override
    public DataStream<RowData> createSource() {
        initColumnInfo();
        initRestoreConfig();
        initPollingConfig();
        initSplitConfig();
        initIncrementConfig();
        JdbcInputFormatBuilder builder = getBuilder();

        int fetchSize = jdbcConfig.getFetchSize();
        jdbcConfig.setFetchSize(fetchSize == 0 ? getDefaultFetchSize() : fetchSize);

        int queryTimeOut = jdbcConfig.getQueryTimeOut();
        jdbcConfig.setQueryTimeOut(queryTimeOut == 0 ? DEFAULT_QUERY_TIMEOUT : queryTimeOut);

        int connectTimeOut = jdbcConfig.getConnectTimeOut();
        jdbcConfig.setConnectTimeOut(
                connectTimeOut == 0 ? DEFAULT_CONNECTION_TIMEOUT : connectTimeOut);

        builder.setJdbcConf(jdbcConfig);
        builder.setJdbcDialect(jdbcDialect);
        builder.setRestoreKeyUtil(splitKeyUtil);
        builder.setIncrementKeyUtil(incrementKeyUtil);
        builder.setSplitKeyUtil(splitKeyUtil);

        builder.setColumnNameList(columnNameList);

        AbstractRowConverter rowConverter;
        if (!useAbstractBaseColumn) {
            checkConstant(jdbcConfig);
            rowConverter =
                    jdbcDialect.getRowConverter(
                            TableUtil.createRowType(jdbcConfig.getColumn(), getRawTypeMapper()));
        } else {
            rowConverter =
                    jdbcDialect.getColumnConverter(
                            TableUtil.createRowType(
                                    columnNameList, columnTypeList, getRawTypeMapper()),
                            jdbcConfig);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);

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

    /** set default split strategy if splitStrategy is blank */
    private void setDefaultSplitStrategy(JdbcConfig jdbcConfig) {
        if (jdbcConfig.getSplitStrategy() == null || jdbcConfig.getSplitStrategy().equals("")) {
            if (jdbcConfig.isIncrement() && jdbcConfig.getParallelism() > 1) {
                jdbcConfig.setSplitStrategy("mod");
            } else {
                jdbcConfig.setSplitStrategy("range");
            }
        }
    }

    protected void initColumnInfo() {
        Connection conn = getConn();
        Pair<List<String>, List<TypeConfig>> tableMetaData = getTableMetaData(conn);
        Pair<List<String>, List<TypeConfig>> selectedColumnInfo =
                JdbcUtil.buildColumnWithMeta(jdbcConfig, tableMetaData, getConstantType());
        JdbcUtil.closeDbResources(null, null, conn, false);
        columnNameList = selectedColumnInfo.getLeft();
        columnTypeList = selectedColumnInfo.getRight();
        this.fieldList = jdbcConfig.getColumn();
    }

    protected Connection getConn() {
        return JdbcUtil.getConnection(jdbcConfig, jdbcDialect);
    }

    protected Pair<List<String>, List<TypeConfig>> getTableMetaData(Connection dbConn) {
        return jdbcDialect.getTableMetaData(dbConn, jdbcConfig);
    }

    protected String getConstantType() {
        return "VARCHAR";
    }

    /** init restore info */
    protected void initRestoreConfig() {
        String name = syncConfig.getRestore().getRestoreColumnName();
        if (StringUtils.isNotBlank(name)) {
            FieldConfig fieldConfig =
                    FieldConfig.getSameNameMetaColumn(jdbcConfig.getColumn(), name);
            if (fieldConfig != null) {
                jdbcConfig.setRestoreColumn(name);
                jdbcConfig.setRestoreColumnIndex(fieldConfig.getIndex());
                jdbcConfig.setRestoreColumnType(fieldConfig.getType().getType());
                restoreKeyUtil =
                        jdbcDialect.initKeyUtil(fieldConfig.getName(), fieldConfig.getType());
            } else {
                throw new IllegalArgumentException("unknown restore column name: " + name);
            }
        }
    }

    protected void initPollingConfig() {
        // The Polling mode does not support range split now
        if (jdbcConfig.isPolling() && jdbcConfig.getParallelism() > 1) {
            jdbcConfig.setSplitStrategy("mod");
        }
    }

    /** 初始化增量或间隔轮询任务配置 */
    private void initIncrementConfig() {
        String increColumn = jdbcConfig.getIncreColumn();

        // 增量字段不为空，表示任务为增量或间隔轮询任务
        if (StringUtils.isNotBlank(increColumn)) {
            List<FieldConfig> fieldConfigList = jdbcConfig.getColumn();
            TypeConfig type = null;
            String name = null;
            int index = -1;

            // 纯数字则表示增量字段在column中的顺序位置
            if (NumberUtils.isCreatable(increColumn)) {
                int idx = Integer.parseInt(increColumn);
                if (idx > fieldConfigList.size() - 1) {
                    throw new ChunJunRuntimeException(
                            String.format(
                                    "config error : incrementColumn must less than column.size() when increColumn is number, column = %s, size = %s, increColumn = %s",
                                    GsonUtil.GSON.toJson(fieldConfigList),
                                    fieldConfigList.size(),
                                    increColumn));
                }
                FieldConfig fieldColumn = fieldConfigList.get(idx);
                type = fieldColumn.getType();
                name = fieldColumn.getName();
                index = fieldColumn.getIndex();
            } else {
                for (FieldConfig field : fieldConfigList) {
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
                                GsonUtil.GSON.toJson(fieldConfigList), increColumn));
            }

            jdbcConfig.setIncrement(true);
            jdbcConfig.setIncreColumn(name);
            jdbcConfig.setIncreColumnType(type.getType());
            jdbcConfig.setIncreColumnIndex(index);

            jdbcConfig.setRestoreColumn(name);
            jdbcConfig.setRestoreColumnType(type.getType());
            jdbcConfig.setRestoreColumnIndex(index);

            incrementKeyUtil = jdbcDialect.initKeyUtil(name, type);
            restoreKeyUtil = incrementKeyUtil;
            initStartLocation();

            if (StringUtils.isBlank(jdbcConfig.getSplitPk())) {
                jdbcConfig.setSplitPk(name);
                splitKeyUtil = incrementKeyUtil;
            }
        }
    }

    public void initSplitConfig() {
        String splitPk = jdbcConfig.getSplitPk();
        if (StringUtils.isNotBlank(splitPk)) {
            jdbcConfig.getColumn().stream()
                    .filter(field -> field.getName().equals(splitPk))
                    .findFirst()
                    .ifPresent(
                            field -> {
                                if (StringUtils.isNotBlank(field.getValue())) {
                                    throw new ChunJunRuntimeException(
                                            "Constant columns are not supported as splitPk");
                                } else {
                                    splitKeyUtil =
                                            jdbcDialect.initKeyUtil(
                                                    field.getName(), field.getType());
                                }
                            });
        }
    }

    protected int getDefaultFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return jdbcDialect.getRawTypeConverter();
    }

    protected void rebuildJdbcConf() {
        // table字段有可能是schema.table格式 需要转换为对应的schema 和 table 字段
        if (StringUtils.isBlank(jdbcConfig.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\\"", "\\\"");
        }
    }

    private void initStartLocation() {
        String startLocation = jdbcConfig.getStartLocation();
        if (StringUtils.isNotBlank(jdbcConfig.getStartLocation())) {
            startLocation =
                    Arrays.stream(startLocation.split(ConstantValue.COMMA_SYMBOL))
                            .map(incrementKeyUtil::checkAndFormatLocationStr)
                            .collect(Collectors.joining(ConstantValue.COMMA_SYMBOL));
        }
        jdbcConfig.setStartLocation(startLocation);
    }
}
