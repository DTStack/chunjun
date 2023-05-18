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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.conf.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.KeyUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.NumericTypeUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.java.tuple.Tuple3;
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

    protected KeyUtil<?, BigInteger> incrementKeyUtil = new NumericTypeUtil();
    protected KeyUtil<?, BigInteger> splitKeyUtil = new NumericTypeUtil();
    protected KeyUtil<?, BigInteger> restoreKeyUtil = new NumericTypeUtil();
    protected List<String> columnNameList;
    protected List<String> columnTypeList;

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

        setDefaultSplitStrategy(jdbcConf);
        super.initCommonConf(jdbcConf);
        if (StringUtils.isBlank(jdbcConf.getCustomSql())) {
            rebuildJdbcConf();
        }
    }

    protected Class<? extends JdbcConf> getConfClass() {
        return JdbcConf.class;
    }

    @Override
    public DataStream<RowData> createSource() {
        initColumnInfo();
        initRestoreConfig();
        initPollingConfig();
        initSplitConfig();
        initIncrementConfig();
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
        builder.setRestoreKeyUtil(splitKeyUtil);
        builder.setIncrementKeyUtil(incrementKeyUtil);
        builder.setSplitKeyUtil(splitKeyUtil);

        builder.setColumnNameList(columnNameList);

        AbstractRowConverter rowConverter;
        if (!useAbstractBaseColumn) {
            checkConstant(jdbcConf);
            rowConverter =
                    jdbcDialect.getRowConverter(
                            TableUtil.createRowType(jdbcConf.getColumn(), getRawTypeConverter()));
        } else {
            rowConverter =
                    jdbcDialect.getColumnConverter(
                            TableUtil.createRowType(
                                    columnNameList, columnTypeList, getRawTypeConverter()),
                            jdbcConf);
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
    private void setDefaultSplitStrategy(JdbcConf jdbcConf) {
        if (jdbcConf.getSplitStrategy() == null || jdbcConf.getSplitStrategy().equals("")) {
            if (jdbcConf.isIncrement() && jdbcConf.getParallelism() > 1) {
                jdbcConf.setSplitStrategy("mod");
            } else {
                jdbcConf.setSplitStrategy("range");
            }
        }
    }

    protected void initColumnInfo() {
        Connection conn = getConn();
        Pair<List<String>, List<String>> tableMetaData = getTableMetaData(conn);
        Pair<List<String>, List<String>> selectedColumnInfo =
                JdbcUtil.buildColumnWithMeta(jdbcConf, tableMetaData, getConstantType());
        JdbcUtil.closeDbResources(null, null, conn, false);
        columnNameList = selectedColumnInfo.getLeft();
        columnTypeList = selectedColumnInfo.getRight();
        this.fieldList = jdbcConf.getColumn();
    }

    protected Connection getConn() {
        return JdbcUtil.getConnection(jdbcConf, jdbcDialect);
    }

    protected Pair<List<String>, List<String>> getTableMetaData(Connection dbConn) {
        Tuple3<String, String, String> tableIdentify =
                jdbcDialect.getTableIdentify(jdbcConf.getSchema(), jdbcConf.getTable());
        return JdbcUtil.getTableMetaData(
                tableIdentify.f0,
                tableIdentify.f1,
                tableIdentify.f2,
                dbConn,
                jdbcConf.getCustomSql());
    }

    protected String getConstantType() {
        return "VARCHAR";
    }

    /** init restore info */
    protected void initRestoreConfig() {
        String name = syncConf.getRestore().getRestoreColumnName();
        if (StringUtils.isNotBlank(name)) {
            FieldConf fieldConf = FieldConf.getSameNameMetaColumn(jdbcConf.getColumn(), name);
            if (fieldConf != null) {
                jdbcConf.setRestoreColumn(name);
                jdbcConf.setRestoreColumnIndex(fieldConf.getIndex());
                jdbcConf.setRestoreColumnType(fieldConf.getType());
                restoreKeyUtil = jdbcDialect.initKeyUtil(fieldConf.getName(), fieldConf.getType());
            } else {
                throw new IllegalArgumentException("unknown restore column name: " + name);
            }
        }
    }

    protected void initPollingConfig() {
        // The Polling mode does not support range split now
        if (jdbcConf.isPolling() && jdbcConf.getParallelism() > 1) {
            jdbcConf.setSplitStrategy("mod");
        }
    }

    /** 初始化增量或间隔轮询任务配置 */
    private void initIncrementConfig() {
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
                    throw new ChunJunRuntimeException(
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

            incrementKeyUtil = jdbcDialect.initKeyUtil(name, type);
            restoreKeyUtil = incrementKeyUtil;
            initStartLocation();

            if (StringUtils.isBlank(jdbcConf.getSplitPk())) {
                jdbcConf.setSplitPk(name);
                splitKeyUtil = incrementKeyUtil;
            }
        }
    }

    public void initSplitConfig() {
        String splitPk = jdbcConf.getSplitPk();
        if (StringUtils.isNotBlank(splitPk)) {
            jdbcConf.getColumn().stream()
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
    public RawTypeConverter getRawTypeConverter() {
        return jdbcDialect.getRawTypeConverter();
    }

    protected void rebuildJdbcConf() {
        // table字段有可能是schema.table格式 需要转换为对应的schema 和 table 字段
        if (StringUtils.isBlank(jdbcConf.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConf, "\\\"", "\\\"");
        }
    }

    private void initStartLocation() {
        String startLocation = jdbcConf.getStartLocation();
        if (StringUtils.isNotBlank(jdbcConf.getStartLocation())) {
            startLocation =
                    Arrays.stream(startLocation.split(ConstantValue.COMMA_SYMBOL))
                            .map(incrementKeyUtil::checkAndFormatLocationStr)
                            .collect(Collectors.joining(ConstantValue.COMMA_SYMBOL));
        }
        jdbcConf.setStartLocation(startLocation);
    }
}
