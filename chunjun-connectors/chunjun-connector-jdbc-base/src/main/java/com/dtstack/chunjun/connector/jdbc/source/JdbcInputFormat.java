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

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.jdbc.util.SqlUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.KeyUtil;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.metrics.BigIntegerAccumulator;
import com.dtstack.chunjun.metrics.StringAccumulator;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** InputFormat for reading data from a database and generate Rows. */
@Slf4j
public class JdbcInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = 2776268462929827L;

    protected static final int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    protected static int resultSetType = ResultSet.TYPE_FORWARD_ONLY;

    protected JdbcConfig jdbcConfig;
    protected JdbcDialect jdbcDialect;

    protected transient Connection dbConn;
    protected transient Statement statement;
    protected transient PreparedStatement ps;
    protected transient ResultSet resultSet;
    protected boolean hasNext;

    protected boolean needUpdateEndLocation;
    protected Object state = null;

    protected StringAccumulator maxValueAccumulator;
    protected BigIntegerAccumulator endLocationAccumulator;
    protected BigIntegerAccumulator startLocationAccumulator;

    // 轮询增量标识字段类型
    protected ColumnType type;

    protected JdbcInputSplit currentJdbcInputSplit;
    protected KeyUtil<?, BigInteger> incrementKeyUtil;
    protected KeyUtil<?, BigInteger> splitKeyUtil;
    private KeyUtil<?, BigInteger> restoreKeyUtil;

    @Override
    public void openInternal(InputSplit inputSplit) {
        this.currentJdbcInputSplit = (JdbcInputSplit) inputSplit;
        initMetric(currentJdbcInputSplit);
        if (!canReadData(currentJdbcInputSplit)) {
            log.warn(
                    "Not read data when the start location are equal to end location, start = {}, end = {}",
                    currentJdbcInputSplit.getStartLocation(),
                    currentJdbcInputSplit.getEndLocation());
            hasNext = false;
            return;
        }

        String querySQL = null;
        try {
            dbConn = getConnection();
            dbConn.setAutoCommit(false);

            querySQL = buildQuerySql(currentJdbcInputSplit);
            jdbcConfig.setQuerySql(querySQL);
            executeQuery(currentJdbcInputSplit.getStartLocation());
            // 增量任务
            needUpdateEndLocation =
                    jdbcConfig.isIncrement()
                            && !jdbcConfig.isPolling()
                            && !jdbcConfig.isUseMaxFunc();
        } catch (SQLException se) {
            String expMsg = se.getMessage();
            expMsg = querySQL == null ? expMsg : expMsg + "\n querySQL: " + querySQL;
            throw new IllegalArgumentException("open() failed." + expMsg, se);
        }
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        if (minNumSplits != jdbcConfig.getParallelism()) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "numTaskVertices is [%s], but parallelism in jdbcConfig is [%s]",
                            minNumSplits, jdbcConfig.getParallelism()));
        }

        if (jdbcConfig.getParallelism() > 1
                && StringUtils.equalsIgnoreCase("range", jdbcConfig.getSplitStrategy())) {
            // splitStrategy = range
            return createSplitsInternalBySplitRange(minNumSplits);
        } else {
            // default,splitStrategy = mod
            return createSplitsInternalBySplitMod(minNumSplits, jdbcConfig.getStartLocation());
        }
    }

    /** Create split for modSplitStrategy */
    public JdbcInputSplit[] createSplitsInternalBySplitMod(int minNumSplits, String startLocation) {
        JdbcInputSplit[] splits = new JdbcInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new JdbcInputSplit(i, minNumSplits, i, "mod", jdbcConfig.isPolling());
        }
        JdbcUtil.setStarLocationForSplits(splits, startLocation);
        log.info("createInputSplitsInternal success, splits is {}", GsonUtil.GSON.toJson(splits));
        return splits;
    }

    @Override
    public boolean reachedEnd() {
        if (hasNext) {
            return false;
        } else {
            if (currentJdbcInputSplit.isPolling()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(jdbcConfig.getPollingInterval());
                    // 间隔轮询检测数据库连接是否断开，超时时间三秒，断开后自动重连
                    if (!dbConn.isValid(3)) {
                        dbConn = getConnection();
                        // 重新连接后还是不可用则认为数据库异常，任务失败
                        if (!dbConn.isValid(3)) {
                            String message =
                                    String.format(
                                            "cannot connect to %s, username = %s, please check %s is available.",
                                            jdbcConfig.getJdbcUrl(),
                                            jdbcConfig.getUsername(),
                                            jdbcDialect.dialectName());
                            throw new ChunJunRuntimeException(message);
                        }
                    }
                    dbConn.setAutoCommit(true);
                    JdbcUtil.closeDbResources(resultSet, null, null, false);
                    queryForPolling(restoreKeyUtil.transToLocationValue(state).toString());
                    return false;
                } catch (InterruptedException e) {
                    log.warn(
                            "interrupted while waiting for polling, e = {}",
                            ExceptionUtil.getErrorMessage(e));
                } catch (SQLException e) {
                    JdbcUtil.closeDbResources(resultSet, ps, null, false);
                    String message =
                            String.format(
                                    "error to execute sql = %s, startLocation = %s, e = %s",
                                    jdbcConfig.getQuerySql(),
                                    state,
                                    ExceptionUtil.getErrorMessage(e));
                    throw new ChunJunRuntimeException(message, e);
                }
            }
            return true;
        }
    }

    @Override
    public RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        if (!hasNext) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            RowData finalRowData = rowConverter.toInternal(resultSet);
            if (needUpdateEndLocation) {
                BigInteger location =
                        incrementKeyUtil.getLocationValueFromRs(
                                resultSet, jdbcConfig.getIncreColumnIndex() + 1);
                endLocationAccumulator.add(location);
                log.debug("update endLocationAccumulator, current Location = {}", location);
            }
            if (jdbcConfig.getRestoreColumnIndex() > -1) {
                state = resultSet.getObject(jdbcConfig.getRestoreColumnIndex() + 1);
            }
            return finalRowData;
        } catch (Exception se) {
            log.error(ExceptionUtil.getErrorMessage(se));
            throw new ReadRecordException("", se, 0, rowData);
        } finally {
            try {
                hasNext = resultSet.next();
            } catch (SQLNonTransientException e) {
                log.error(ExceptionUtil.getErrorMessage(e));
                throw new ChunJunRuntimeException(e);
            } catch (SQLException e) {
                log.error("can not read next record", e);
                hasNext = false;
            }
        }
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        formatState.setState(state);
        return formatState;
    }

    @Override
    public void closeInternal() {
        JdbcUtil.closeDbResources(resultSet, statement, dbConn, true);
    }

    /**
     * 初始化增量或或间隔轮询任务累加器
     *
     * @param inputSplit 数据分片
     */
    protected void initMetric(InputSplit inputSplit) {
        if (!jdbcConfig.isIncrement()) {
            return;
        }
        // 初始化增量、轮询字段类型
        type = ColumnType.fromString(jdbcConfig.getIncreColumnType());
        startLocationAccumulator = new BigIntegerAccumulator();
        endLocationAccumulator = new BigIntegerAccumulator();
        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;

        // get maxValue in task to avoid timeout in SourceFactory
        if ((jdbcConfig.isPolling() && jdbcConfig.isPollingFromMax())
                || (!jdbcConfig.isPolling() && jdbcConfig.isUseMaxFunc())) {
            String maxValue = getMaxValue(inputSplit);
            if (jdbcConfig.isPolling()) {
                ((JdbcInputSplit) inputSplit).setStartLocation(maxValue);
            } else {
                ((JdbcInputSplit) inputSplit).setEndLocation(maxValue);
                endLocationAccumulator.add(new BigInteger(maxValue));
            }
        }

        String startLocation = jdbcInputSplit.getStartLocation();
        if (StringUtils.isNotBlank(startLocation)) {
            startLocationAccumulator.add(new BigInteger(startLocation));
            // 防止数据库无增量数据时下次从prometheus获取到的startLocation为空
            if (endLocationAccumulator.getLocalValue().intValue()
                    == BigIntegerAccumulator.MIN_VAL) {
                endLocationAccumulator.add(new BigInteger(startLocation));
            }
        }

        // 将累加器信息添加至prometheus
        if (useCustomReporter()) {
            customReporter.registerMetric(startLocationAccumulator, Metrics.START_LOCATION);
            customReporter.registerMetric(endLocationAccumulator, Metrics.END_LOCATION);
        }
        if (getRuntimeContext().getAccumulator(Metrics.START_LOCATION) == null) {
            getRuntimeContext().addAccumulator(Metrics.START_LOCATION, startLocationAccumulator);
        }
        if (getRuntimeContext().getAccumulator(Metrics.END_LOCATION) == null) {
            getRuntimeContext().addAccumulator(Metrics.END_LOCATION, endLocationAccumulator);
        }
    }

    /**
     * 将增量任务的数据最大值设置到累加器中
     *
     * @param inputSplit 数据分片
     */
    protected String getMaxValue(InputSplit inputSplit) {
        String maxValue;
        if (inputSplit.getSplitNumber() == 0) {
            maxValue = incrementKeyUtil.transToLocationValue(getMaxValueFromDb()).toString();
            // 将累加器信息上传至flink，供其他通道通过flink rest api获取该最大值
            maxValueAccumulator = new StringAccumulator();
            maxValueAccumulator.add(maxValue);
            getRuntimeContext().addAccumulator(Metrics.MAX_VALUE, maxValueAccumulator);
        } else {
            int times = 0;
            do {
                maxValue = accumulatorCollector.getRdbMaxFuncValue();
            } while (maxValue.equals(Metrics.MAX_VALUE_NONE) && times++ < 6);
            if (maxValue.equals(Metrics.MAX_VALUE_NONE)) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "failed to get maxValue by accumulator,splitNumber[%s]",
                                inputSplit.getSplitNumber()));
            }
        }
        return maxValue;
    }

    /**
     * 从数据库中查询增量字段的最大值
     *
     * @return
     */
    private String getMaxValueFromDb() {
        String maxValue = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            long startTime = System.currentTimeMillis();

            String queryMaxValueSql;
            if (StringUtils.isNotEmpty(jdbcConfig.getCustomSql())) {
                queryMaxValueSql =
                        String.format(
                                "select max(%s.%s) as max_value from ( %s ) %s",
                                JdbcUtil.TEMPORARY_TABLE_NAME,
                                jdbcDialect.quoteIdentifier(jdbcConfig.getIncreColumn()),
                                jdbcConfig.getCustomSql(),
                                JdbcUtil.TEMPORARY_TABLE_NAME);
            } else {
                queryMaxValueSql =
                        String.format(
                                "select max(%s) as max_value from %s",
                                jdbcDialect.quoteIdentifier(jdbcConfig.getIncreColumn()),
                                jdbcDialect.quoteIdentifier(jdbcConfig.getTable()));
            }

            String startSql =
                    buildStartLocationSql(
                            jdbcDialect.quoteIdentifier(jdbcConfig.getIncreColumn()),
                            jdbcConfig.getStartLocation(),
                            jdbcConfig.isUseMaxFunc(),
                            jdbcConfig.isPolling());
            if (StringUtils.isNotEmpty(startSql)) {
                queryMaxValueSql += " where " + startSql;
            }

            log.info(String.format("Query max value sql is '%s'", queryMaxValueSql));

            conn = getConnection();
            st = conn.createStatement(resultSetType, resultSetConcurrency);
            st.setQueryTimeout(jdbcConfig.getQueryTimeOut());
            rs = st.executeQuery(queryMaxValueSql);
            if (rs.next()) {
                maxValue = incrementKeyUtil.getLocationValueFromRs(rs, "max_value").toString();
            }

            log.info(
                    String.format(
                            "Takes [%s] milliseconds to get the maximum value [%s]",
                            System.currentTimeMillis() - startTime, maxValue));

            return maxValue;
        } catch (Throwable e) {
            throw new RuntimeException("Get max value from " + jdbcConfig.getTable() + " error", e);
        } finally {
            JdbcUtil.closeDbResources(rs, st, conn, false);
        }
    }

    /**
     * 从数据库中查询切割键的最大 最小值
     *
     * @return
     */
    private Pair<String, String> getSplitRangeFromDb() {
        Pair<String, String> splitPkRange = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            long startTime = System.currentTimeMillis();

            String querySplitRangeSql = SqlUtil.buildQuerySplitRangeSql(jdbcConfig, jdbcDialect);
            log.info(String.format("Query SplitRange sql is '%s'", querySplitRangeSql));

            conn = getConnection();
            st = conn.createStatement(resultSetType, resultSetConcurrency);
            st.setQueryTimeout(jdbcConfig.getQueryTimeOut());
            rs = st.executeQuery(querySplitRangeSql);
            if (rs.next()) {
                splitPkRange =
                        Pair.of(
                                String.valueOf(rs.getObject("min_value")),
                                String.valueOf(rs.getObject("max_value")));
            }

            log.info(
                    String.format(
                            "Takes [%s] milliseconds to get the SplitRange value [%s]",
                            System.currentTimeMillis() - startTime, splitPkRange));

            return splitPkRange;
        } catch (Throwable e) {
            throw new ChunJunRuntimeException(
                    "Get SplitRange value from " + jdbcConfig.getTable() + " error", e);
        } finally {
            JdbcUtil.closeDbResources(rs, st, conn, false);
        }
    }

    /**
     * 判断增量任务是否还能继续读取数据 增量任务，startLocation = endLocation且两者都不为null，返回false，其余情况返回true
     *
     * @param jdbcInputSplit 数据分片
     * @return
     */
    protected boolean canReadData(JdbcInputSplit jdbcInputSplit) {
        // 只排除增量同步
        if (!jdbcConfig.isIncrement() || currentJdbcInputSplit.isPolling()) {
            return true;
        }

        if (jdbcInputSplit.getStartLocation() == null && jdbcInputSplit.getEndLocation() == null) {
            return true;
        }

        return !StringUtils.equals(
                jdbcInputSplit.getStartLocation(), jdbcInputSplit.getEndLocation());
    }

    /**
     * 构造查询sql
     *
     * @param inputSplit 数据切片
     * @return 构建的sql字符串
     */
    protected String buildQuerySql(InputSplit inputSplit) {
        List<String> whereList = new ArrayList<>();

        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;
        buildLocationFilter(jdbcInputSplit, whereList);

        if (StringUtils.isNotBlank(jdbcConfig.getWhere())) {
            whereList.add(jdbcConfig.getWhere());
        }
        String querySql;

        querySql = buildQuerySqlBySplit(jdbcInputSplit, whereList);

        String finalSql = SqlUtil.buildOrderSql(querySql, jdbcConfig, jdbcDialect, "ASC");
        log.info("Executing sql is: '{}'", finalSql);
        return finalSql;
    }

    /**
     * 构建起始位置sql
     *
     * @param incrementCol 增量字段名称
     * @param startLocation 开始位置
     * @param useMaxFunc 是否保存结束位置数据
     * @return
     */
    public String buildStartLocationSql(
            String incrementCol, String startLocation, boolean useMaxFunc, boolean isPolling) {
        if (StringUtils.isEmpty(startLocation)
                || JdbcUtil.NULL_STRING.equalsIgnoreCase(startLocation)) {
            return null;
        }

        String operator = useMaxFunc ? " >= " : " > ";

        // 增量轮询，startLocation使用占位符代替
        if (isPolling) {
            return incrementCol + operator + "?";
        }

        return getLocationSql(incrementCol, startLocation, operator);
    }

    /**
     * 构建边界位置sql
     *
     * @param incrementCol 增量字段名称
     * @param location 边界位置(起始/结束)
     * @param operator 判断符( >, >=, <)
     * @return
     */
    protected String getLocationSql(String incrementCol, String location, String operator) {
        String endLocationSql;

        endLocationSql =
                incrementCol
                        + operator
                        + incrementKeyUtil.transLocationStrToStatementValue(location);

        return endLocationSql;
    }

    /**
     * 增量轮询查询
     *
     * @param startLocation
     * @throws SQLException
     */
    protected void queryForPolling(String startLocation) throws SQLException {
        // 每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
        if ((System.currentTimeMillis() - startTime) % 300000 <= jdbcConfig.getPollingInterval()) {
            log.info("polling startLocation = {}", startLocation);
        } else {
            log.debug("polling startLocation = {}", startLocation);
        }

        incrementKeyUtil.setPsWithLocationStr(ps, 1, startLocation);
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();
    }

    /** 构建基于startLocation&endLocation的过滤条件 * */
    protected void buildLocationFilter(JdbcInputSplit jdbcInputSplit, List<String> whereList) {
        if (formatState.getState() != null
                && StringUtils.isNotBlank(jdbcConfig.getRestoreColumn())) {
            if (StringUtils.isNotBlank(String.valueOf(formatState.getState()))) {
                log.info("restore from checkpoint with state{}", formatState.getState());
                if (jdbcConfig.isIncrement()) {
                    jdbcInputSplit.setStartLocation(
                            incrementKeyUtil
                                    .transToLocationValue(formatState.getState())
                                    .toString());
                }
                whereList.add(
                        buildFilterSql(
                                jdbcConfig.getCustomSql(),
                                ">",
                                jdbcDialect.quoteIdentifier(jdbcConfig.getRestoreColumn()),
                                jdbcInputSplit.isPolling(),
                                restoreKeyUtil.transToStatementValue(formatState.getState())));
            }
        } else if (jdbcConfig.isIncrement()) {
            String startLocation = jdbcInputSplit.getStartLocation();
            if (StringUtils.isNotBlank(startLocation)) {
                String operator = jdbcConfig.isUseMaxFunc() ? " >= " : " > ";
                whereList.add(
                        buildFilterSql(
                                jdbcConfig.getCustomSql(),
                                operator,
                                jdbcDialect.quoteIdentifier(jdbcConfig.getIncreColumn()),
                                jdbcInputSplit.isPolling(),
                                incrementKeyUtil.transLocationStrToStatementValue(
                                        jdbcInputSplit.getStartLocation())));
            }
            if (StringUtils.isNotBlank(jdbcInputSplit.getEndLocation())) {
                String operator = jdbcConfig.isUseMaxFunc() ? " < " : " <= ";
                whereList.add(
                        buildFilterSql(
                                jdbcConfig.getCustomSql(),
                                operator,
                                jdbcDialect.quoteIdentifier(jdbcConfig.getIncreColumn()),
                                false,
                                incrementKeyUtil.transLocationStrToStatementValue(
                                        jdbcInputSplit.getEndLocation())));
            }
        }
    }

    /** create querySql for inputSplit * */
    protected String buildQuerySqlBySplit(JdbcInputSplit jdbcInputSplit, List<String> whereList) {
        return SqlUtil.buildQuerySqlBySplit(
                jdbcConfig, jdbcDialect, whereList, columnNameList, jdbcInputSplit);
    }

    /** create split for rangeSplitStrategy */
    protected JdbcInputSplit[] createSplitsInternalBySplitRange(int minNumSplits) {
        List<JdbcInputSplit> splits = new ArrayList<>();
        Pair<String, String> splitRangeFromDb = getSplitRangeFromDb();
        BigDecimal left, right = null;
        if (StringUtils.isNotBlank(splitRangeFromDb.getLeft())
                && !"null".equalsIgnoreCase(splitRangeFromDb.getLeft())) {
            left = new BigDecimal(splitKeyUtil.transToLocationValue(splitRangeFromDb.getLeft()));
            right = new BigDecimal(splitKeyUtil.transToLocationValue(splitRangeFromDb.getRight()));
            splits.addAll(createRangeSplits(left, right, minNumSplits));
            if (jdbcConfig.isPolling()) {
                // rangeSplit in polling mode,range first then mod.we need to change the last range
                // shard here to <= endLocationOfSplit
                splits.get(splits.size() - 1).setRangeEndLocationOperator(" <= ");
            }
        }
        // create modSplit for polling
        if (jdbcConfig.isPolling()) {
            splits.addAll(
                    Arrays.asList(
                            createSplitsInternalBySplitMod(
                                    jdbcConfig.getParallelism(),
                                    right == null
                                            ? jdbcConfig.getStartLocation()
                                            : String.valueOf(right))));
        }
        // Reverse,when pollingMode configures rangeStrategy, be sure to do rangeSplit first, then
        // modSplit
        Collections.reverse(splits);
        return splits.toArray(new JdbcInputSplit[0]);
    }

    protected List<JdbcInputSplit> createRangeSplits(
            BigDecimal left, BigDecimal right, int minNumSplits) {
        BigDecimal endAndStartGap = right.subtract(left);
        if (endAndStartGap.compareTo(BigDecimal.ZERO) < 0) return new ArrayList<>();
        JdbcInputSplit[] splits;
        log.info("create splitsInternal,the splitKey range is {} --> {}", left, right);
        BigDecimal remainder = endAndStartGap.remainder(new BigDecimal(minNumSplits));
        endAndStartGap = endAndStartGap.subtract(remainder);
        BigDecimal step = endAndStartGap.divide(new BigDecimal(minNumSplits));

        if (step.compareTo(BigDecimal.ZERO) == 0) {
            // if left = right，step and remainder is 0
            if (remainder.compareTo(BigDecimal.ZERO) == 0) {
                minNumSplits = 1;
            } else {
                minNumSplits = remainder.intValue();
            }
        }

        splits = new JdbcInputSplit[minNumSplits];
        BigDecimal start;
        BigDecimal end = left;
        for (int i = 0; i < minNumSplits; i++) {
            start = end;
            end = start.add(step);
            if (remainder.compareTo(BigDecimal.ZERO) > 0) {
                end = end.add(BigDecimal.ONE);
                remainder = remainder.subtract(BigDecimal.ONE);
            }
            // incrementalMode,The final rangeSplit scope is splitPk >= start
            // pollingMode,The final rangeSplit scope is splitPk >= start and splitPk < =end
            if (i == minNumSplits - 1) {
                if (jdbcConfig.isPolling()) {
                    end = right;
                } else {
                    end = null;
                }
            }
            splits[i] =
                    new JdbcInputSplit(
                            i,
                            minNumSplits,
                            i,
                            jdbcConfig.getStartLocation(),
                            null,
                            splitKeyUtil.transLocationStrToStatementValue(start.toString()),
                            Objects.isNull(end)
                                    ? null
                                    : splitKeyUtil.transLocationStrToStatementValue(end.toString()),
                            "range",
                            false);
        }

        return Arrays.asList(splits);
    }

    /**
     * 执行查询
     *
     * @param startLocation
     * @throws SQLException
     */
    protected void executeQuery(String startLocation) throws SQLException {
        if (currentJdbcInputSplit.isPolling()) {
            if (StringUtils.isBlank(startLocation)) {
                // avoid startLocation is null
                queryPollingWithOutStartLocation();
                // Concatenated sql statement for next polling query
                StringBuilder builder = new StringBuilder(128);
                builder.append(jdbcConfig.getQuerySql());
                if (jdbcConfig.getQuerySql().contains("WHERE")) {
                    builder.append(" AND ");
                } else {
                    builder.append(" WHERE ");
                }
                builder.append(jdbcDialect.quoteIdentifier(jdbcConfig.getIncreColumn()))
                        .append(" > ?");

                String querySQL =
                        SqlUtil.buildOrderSql(builder.toString(), jdbcConfig, jdbcDialect, "ASC");
                jdbcConfig.setQuerySql(querySQL);
                initPrepareStatement(jdbcConfig.getQuerySql());
                log.info("update querySql, sql = {}", jdbcConfig.getQuerySql());
            } else {
                // if the job have startLocation
                // sql will be like "select ... from ... where increColumn > ?"
                jdbcConfig.setQuerySql(
                        SqlUtil.buildOrderSql(
                                jdbcConfig.getQuerySql(), jdbcConfig, jdbcDialect, "ASC"));
                initPrepareStatement(jdbcConfig.getQuerySql());
                queryForPolling(startLocation);
                state = restoreKeyUtil.transLocationStrToSqlValue(startLocation);
            }
        } else {
            statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
            if (jdbcConfig.isDefineColumnTypeForStatement()
                    && StringUtils.isBlank(jdbcConfig.getCustomSql())) {
                defineColumnType(statement);
            }
            statement.setFetchSize(jdbcConfig.getFetchSize());
            statement.setQueryTimeout(jdbcConfig.getQueryTimeOut());
            resultSet = statement.executeQuery(jdbcConfig.getQuerySql());
            hasNext = resultSet.next();
        }
    }

    protected void defineColumnType(Statement statement) throws SQLException {}

    /** init prepareStatement */
    public void initPrepareStatement(String querySql) throws SQLException {
        ps = dbConn.prepareStatement(querySql, resultSetType, resultSetConcurrency);
        ps.setFetchSize(jdbcConfig.getFetchSize());
        ps.setQueryTimeout(jdbcConfig.getQueryTimeOut());
    }

    /**
     * polling mode first query when startLocation is not set
     *
     * @throws SQLException
     */
    protected void queryPollingWithOutStartLocation() throws SQLException {
        // add order by to query SQL avoid duplicate data
        initPrepareStatement(
                SqlUtil.buildOrderSql(jdbcConfig.getQuerySql(), jdbcConfig, jdbcDialect, "ASC"));
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();

        try {
            // 间隔轮询一直循环，直到查询到数据库中的数据为止
            while (!hasNext) {
                TimeUnit.MILLISECONDS.sleep(jdbcConfig.getPollingInterval());
                resultSet.close();
                // 如果事务不提交 就会导致数据库即使插入数据 也无法读到数据
                dbConn.commit();
                resultSet = ps.executeQuery();
                hasNext = resultSet.next();
                // 每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
                if ((System.currentTimeMillis() - startTime) % 300000
                        <= jdbcConfig.getPollingInterval()) {
                    log.info(
                            "no record matched condition in database, execute query sql = {}, startLocation = {}",
                            jdbcConfig.getQuerySql(),
                            endLocationAccumulator.getLocalValue());
                }
            }
        } catch (InterruptedException e) {
            log.warn(
                    "interrupted while waiting for polling, e = {}",
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 构造过滤条件SQL
     *
     * @param operator 比较符
     * @param location 比较的值
     * @param columnName 字段名称
     * @param isPolling 是否是轮询任务
     * @return
     */
    public String buildFilterSql(
            String customSql,
            String operator,
            String columnName,
            boolean isPolling,
            String location) {
        StringBuilder sql = new StringBuilder(64);
        if (StringUtils.isNotEmpty(customSql)) {
            sql.append(JdbcUtil.TEMPORARY_TABLE_NAME).append(".");
        }
        sql.append(columnName).append(" ").append(operator).append(" ");
        if (isPolling) {
            // 轮询任务使用占位符
            sql.append("?");
        } else {
            sql.append(location);
        }

        return sql.toString();
    }

    /**
     * 获取数据库连接，用于子类覆盖
     *
     * @return connection
     */
    protected Connection getConnection() throws SQLException {
        return JdbcUtil.getConnection(jdbcConfig, jdbcDialect);
    }

    /** 使用自定义的指标输出器把增量指标打到普罗米修斯 */
    @Override
    protected boolean useCustomReporter() {
        return jdbcConfig.isIncrement() && jdbcConfig.getInitReporter();
    }

    /** 为了保证增量数据的准确性，指标输出失败时使任务失败 */
    @Override
    protected boolean makeTaskFailedWhenReportFailed() {
        return true;
    }

    public JdbcConfig getJdbcConfig() {
        return jdbcConfig;
    }

    public void setJdbcConf(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    public JdbcDialect getJdbcDialect() {
        return jdbcDialect;
    }

    public void setJdbcDialect(JdbcDialect jdbcDialect) {
        this.jdbcDialect = jdbcDialect;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public void setIncrementKeyUtil(KeyUtil<?, BigInteger> incrementKeyUtil) {
        this.incrementKeyUtil = incrementKeyUtil;
    }

    public void setSplitKeyUtil(KeyUtil<?, BigInteger> splitKeyUtil) {
        this.splitKeyUtil = splitKeyUtil;
    }

    public void setRestoreKeyUtil(KeyUtil<?, BigInteger> splitKeyUtil) {
        this.restoreKeyUtil = splitKeyUtil;
    }
}
